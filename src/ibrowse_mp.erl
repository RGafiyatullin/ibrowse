%%%-------------------------------------------------------------------
%%% File    : ibrowse_mp.erl
%%% Author  : Roman Gafiyatullin <r.gafiyatullin@me.com>
%%% Description : A multipipeline server
%%%
%%% It may be needed to have multiple pipelines to a single endpoint.
%%% For instance, the expected response times of different 
%%%  API-calls may vary. Hence having them in the same pipeline makes
%%%  the quicker tasks lag due to head-of-line blocking.
%%%-------------------------------------------------------------------
-module (ibrowse_mp).
-behaviour (gen_server).
-export ([
		start_link/3, start_link/4,
		pl_spec/1,
		send_req/5, send_req/6, send_req/7, send_req/8
	]).
-export ([
		get_pl/2,
		start_link_lb/3
	]).
-export ([
		init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3
	]).

-type host() :: string().
-type tcp_port() :: non_neg_integer().
-type pipe_name() :: term().
-record(pipeline_spec, {
		name :: pipe_name(),
		max_len :: pos_integer(),
		max_sessions :: pos_integer()
	}).
-type pipeline_spec() :: #pipeline_spec{}.
-type reg() :: {local, atom()} | {global, term()}.
-spec start_link( reg(), Host :: host(), Port :: tcp_port(), Pipelines :: [ pipeline_spec() ] ) -> {ok, pid()}.
start_link( Reg = {_Via, _RegName}, Host, Port, Pipelines ) ->
	gen_server:start_link( Reg, ?MODULE, { ibrowse_mp_srv, Host, Port, Pipelines }, [] ).

-spec start_link( Host :: host(), Port :: tcp_port(), Pipelines :: [ pipeline_spec() ] ) -> {ok, pid()}.
start_link( Host, Port, Pipelines ) ->
	gen_server:start_link( ?MODULE, { ibrowse_mp_srv, Host, Port, Pipelines }, [] ).

-spec get_pl( MPSrv :: pid(), PName :: pipe_name() ) ->
	  {ok, LbPid :: pid() | atom(), MaxSessions :: pos_integer(), MaxPipeLen :: pos_integer()}
	| {error, not_found}.
get_pl( MPSrv, PName ) when ( is_pid( MPSrv ) orelse is_atom( MPSrv ) ) -> gen_server:call( MPSrv, {get_pl, PName} ).

send_req( MPSrv, PName, Url, Headers, Method ) -> send_req( MPSrv, PName, Url, Headers, Method, [] ).
send_req( MPSrv, PName, Url, Headers, Method, Body ) -> send_req( MPSrv, PName, Url, Headers, Method, Body, [] ).
send_req( MPSrv, PName, Url, Headers, Method, Body, Options ) -> send_req( MPSrv, PName, Url, Headers, Method, Body, Options, 30000 ).
send_req( MPSrv, PName, Url, Headers, Method, Body, Options, Timeout ) ->
	case get_pl( MPSrv, PName ) of
		{ok, LbPid, MaxSessions, MaxPipeLen} ->
			ibrowse:send_req_via_lb( {LbPid, MaxSessions, MaxPipeLen}, Url, Headers, Method, Body, Options, Timeout );
		Error -> Error
	end.

-spec start_link_sup( host(), tcp_port() ) -> {ok, pid()}.
start_link_sup( Host, Port ) ->
	supervisor:start_link( ?MODULE, { ibrowse_mp_sup, Host, Port } ).

-spec start_link_lb( Host :: host(), Port :: tcp_port(), PName :: pipe_name() ) -> {ok, pid()}.
start_link_lb( Host, Port, _PName ) ->
	ibrowse_lb:start_link([ Host, Port ]).

pl_spec(Props) ->
	#pipeline_spec{
			name = proplists_get_mandatory_value( name, Props ),
			max_len = proplists:get_value( max_len, Props, 30 ),
			max_sessions = proplists:get_value( max_sessions, Props, 10 )
		}.
-record(pipeline, {
		name :: pipe_name(),
		spec :: pipeline_spec(),
		lb_pid :: pid(),
		lb_mon_ref :: undefined | reference()
	}).
-record(s, {
		pipelines :: [ {pipe_name(), pipeline_spec(), LbPid :: pid()} ],
		sup :: pid()
	}).

init( {ibrowse_mp_srv, Host, Port, PipelineSpecs} ) ->
	{ok, Sup} = start_link_sup( Host, Port ),
	Pipelines = lists:map(
		init_pipeline_fun( Sup ), PipelineSpecs ),
	ok = lists:foreach( fun pd_pipeline_store/1, Pipelines ),
	{ok, #s{ pipelines = Pipelines, sup = Sup }};

init( {ibrowse_mp_sup, Host, Port} ) ->
	{ok, { {simple_one_for_one, 0, 1}, [
			{lb, { ?MODULE, start_link_lb, [ Host, Port ] }, 
				temporary, 1000, worker, [ ibrowse_lb ] }
		] }}.

handle_call( {get_pl, PName}, _GenReplyTo, State ) ->
	Reply = 
		case erlang:get( pipeline_key( PName ) ) of
			undefined -> {error, not_found};
			#pipeline{ lb_pid = LbPid, spec = #pipeline_spec{ max_sessions = MaxSessions, max_len = MaxPipeLen } } ->
				{ok, LbPid, MaxSessions, MaxPipeLen}
		end,
	{reply, Reply, State};

handle_call( Unexpected, GenReplyTo, State ) ->
	error_logger:warning_report([
		?MODULE, handle_call,
		{unexpected, Unexpected},
		{gen_reply_to, GenReplyTo} ]),
	{reply, badarg, State}.

handle_cast( Unexpected, State ) ->
	error_logger:warning_report([
		?MODULE, handle_cast,
		{unexpected, Unexpected} ]),
	{noreply, State}.

handle_info(
	DownMsg = {'DOWN', _MonRef, process, LbPid, Reason},
	State = #s{ pipelines = Pipelines0, sup = Sup }
) ->
	case lists:keytake( LbPid, #pipeline.lb_pid, Pipelines0 ) of
		false ->
			error_logger:warning_report([
				?MODULE, handle_info,
				{unexpected, DownMsg} ]),
			{noreply, State};
		{value, #pipeline{ name = PName, spec = PSpec }, Pipelines1} ->
			ok = pd_pipeline_erase( PName ),
			case Reason of
				shutdown -> {noreply, State #s{ pipelines = Pipelines1 }};
				{shutdown, _} -> {noreply, State #s{ pipelines = Pipelines1 }};
				_ ->
					error_logger:warning_report([
						?MODULE, handle_info_down,
						pipeline_lb_dead,
						{reason, Reason}, {pid, LbPid},
						respawning ]),
					RespawnedPipeline = (init_pipeline_fun( Sup ))( PSpec ),
					ok = pd_pipeline_store( RespawnedPipeline ),
					Pipelines2 = [ RespawnedPipeline | Pipelines1 ],
					{noreply, State #s{ pipelines = Pipelines2 }}
			end
	end;
handle_info( Unexpected, State ) ->
	error_logger:warning_report([
		?MODULE, handle_info,
		{unexpected, Unexpected} ]),
	{noreply, State}.

terminate( _Reason, _State ) -> ignore.

code_change( _OldVsn, State, _Extra ) -> {ok, State}.

%%% Internal %%%
proplists_get_mandatory_value( Key, Props ) ->
	Ref = make_ref(),
	case proplists:get_value( Key, Props, Ref ) of
		Ref -> error({mandatory_property_missing, Key});
		Value -> Value
	end.

-spec pd_pipeline_store( #pipeline{} ) -> ok.
pd_pipeline_store( Pipeline = #pipeline{ name = PName, lb_pid = LbPid } ) ->
	MonRef = erlang:monitor( process, LbPid ),
	undefined = erlang:put(
		pipeline_key( PName ),
		Pipeline #pipeline{ lb_mon_ref = MonRef } ),
	ok.

-spec pd_pipeline_erase( PName :: pipe_name() ) -> ok.
pd_pipeline_erase( PName ) ->
	#pipeline{} = erlang:erase( pipeline_key( PName ) ),
	ok.

-spec pipeline_key( PName :: pipe_name() ) -> term().
pipeline_key( PName ) -> [ pipeline | PName ].

-spec init_pipeline_fun( Sup :: pid() ) -> fun( ( pipeline_spec() ) -> #pipeline{} ).
init_pipeline_fun( Sup ) ->
	fun(PSpec = #pipeline_spec{ name = PName } ) ->
		{ok, LbPid} = supervisor:start_child( Sup, [ PName ] ),
		#pipeline{ name = PName, spec = PSpec, lb_pid = LbPid }
	end.
	
