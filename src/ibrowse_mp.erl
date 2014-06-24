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
		send_req/5, send_req/6, send_req/7, send_req/8
	]).
-export ([
		get_pl/2,
		unset_pl/2,
		set_pl/2,
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
-type pl_prop() :: { name, pipe_name() } | {max_sessions, pos_integer()} | {max_len, pos_integer()}.
-type pl_props() :: [ pl_prop() ].
-record(pipeline_spec, {
		name :: pipe_name(),
		max_len :: pos_integer(),
		max_sessions :: pos_integer()
	}).
-record(pipeline_redirect_spec, {
		name :: pipe_name(),
		pass_to :: pipe_name()
	}).
-type pipeline_spec() :: #pipeline_spec{} | #pipeline_redirect_spec{}.
-type reg() :: {local, atom()} | {global, term()}.
-spec start_link( reg(), Host :: host(), Port :: tcp_port(), PipelineProps :: [ pl_props() ] ) -> {ok, pid()}.
start_link( Reg = {_Via, _RegName}, Host, Port, PipelineProps ) ->
	gen_server:start_link( Reg, ?MODULE, { ibrowse_mp_srv, Host, Port, PipelineProps }, [] ).

-spec start_link( Host :: host(), Port :: tcp_port(), PipelineProps :: [ pl_props() ] ) -> {ok, pid()}.
start_link( Host, Port, PipelineProps ) ->
	gen_server:start_link( ?MODULE, { ibrowse_mp_srv, Host, Port, PipelineProps }, [] ).

-spec get_pl( MPSrv :: pid(), PName :: pipe_name() ) ->
	  {ok, LbPid :: pid() | atom(), MaxSessions :: pos_integer(), MaxPipeLen :: pos_integer()}
	| {error, not_found}.
get_pl( MPSrv, PName ) when ( is_pid( MPSrv ) orelse is_atom( MPSrv ) ) -> gen_server:call( MPSrv, {get_pl, PName} ).
set_pl( MPSrv, PLProps ) when ( is_pid( MPSrv ) orelse is_atom( MPSrv ) ) andalso is_list( PLProps ) ->
	PLSpec = pl_spec( PLProps ),
	gen_server:call( MPSrv, {set_pl, PLSpec} ).

unset_pl( MPSrv, PName ) when ( is_pid( MPSrv ) orelse is_atom( MPSrv ) ) -> gen_server:call( MPSrv, {unset_pl, PName} ).

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

-spec pl_spec( pl_props() ) -> pipeline_spec().
pl_spec(Props) ->
	case proplists:get_value( redirect, Props ) of
		undefined ->
			#pipeline_spec{
					name = proplists_get_mandatory_value( name, Props ),
					max_len = proplists:get_value( max_len, Props, 30 ),
					max_sessions = proplists:get_value( max_sessions, Props, 10 )
				};
		Defined ->
			#pipeline_redirect_spec{
					name = proplists_get_mandatory_value( name, Props ),
					pass_to = Defined
				}
	end.
-record(pipeline, {
		name :: pipe_name(),
		spec :: pipeline_spec(),
		lb_pid :: pid(),
		lb_mon_ref :: undefined | reference()
	}).
-type pipeline() :: #pipeline{}.
-record(s, {
		pipelines :: [ pipeline() ],
		sup :: pid()
	}).

init( {ibrowse_mp_srv, Host, Port, PipelineProps} ) ->
	{ok, Sup} = start_link_sup( Host, Port ),
	PipelineSpecs = lists:map(
		fun pl_spec/1, PipelineProps ),
	Pipelines = lists:map(
		init_pipeline_fun( Sup ), PipelineSpecs ),
	ok = lists:foreach( fun pd_pipeline_store/1, Pipelines ),
	{ok, #s{ pipelines = Pipelines, sup = Sup }};

init( {ibrowse_mp_sup, Host, Port} ) ->
	{ok, { {simple_one_for_one, 0, 1}, [
			{lb, { ?MODULE, start_link_lb, [ Host, Port ] }, 
				temporary, 1000, worker, [ ibrowse_lb ] }
		] }}.

handle_call( {get_pl, PName}, GenReplyTo, State ) ->
	case erlang:get( pipeline_key( PName ) ) of
		undefined -> {reply, {error, not_found}, State};
		#pipeline{ lb_pid = LbPid, spec = #pipeline_spec{ max_sessions = MaxSessions, max_len = MaxPipeLen } } ->
			{reply, {ok, LbPid, MaxSessions, MaxPipeLen}, State};
		#pipeline{ spec = #pipeline_redirect_spec{ pass_to = PassToPName } } ->
			handle_call( {get_pl, PassToPName}, GenReplyTo, State )
	end;

% handle_call( {set_pl, PLSpec = #pipeline_spec{ name = PName }}, _GenReplyTo, State = #s{ sup = Sup, pipelines = Pipelines0 } ) ->
% 	case erlang:get( pipeline_key(PName) ) of
% 		undefined ->
% 			Pipeline = (init_pipeline_fun(Sup))( PLSpec ),
% 			ok = pd_pipeline_store( Pipeline ),
% 			{reply, {ok, created}, State #s{ pipelines = [ Pipeline | Pipelines0 ] }};

% 		P0 = #pipeline{ spec = #pipeline_spec{} } ->
% 			P1 = P0 #pipeline{ spec = PLSpec },
% 			_ = erlang:put( pipeline_key(PName), P1 ),
% 			Pipelines1 = lists:keyreplace( PName, #pipeline.name, Pipelines0, P1 ),
% 			{reply, {ok, updated}, State #s{ pipelines = Pipelines1 }};

% 		#pipeline{ spec = #pipeline_redirect_spec{ pass_to = PassToPName } } ->
% 			{reply, {error, {redirect, PassToPName}}, State}
% 	end;
handle_call( {set_pl, PLSpec = #pipeline_redirect_spec{ name = PName }}, _GenReplyTo, State = #s{} ) ->
	case erlang:get( pipeline_key( PName ) ) of
		undefined -> set_pl_create_redirect( PLSpec, State );
		P = #pipeline{ spec = #pipeline_redirect_spec{} } -> set_pl_update_redirect( PLSpec, P, State );
		P = #pipeline{ spec = #pipeline_spec{} } -> set_pl_replace_real_with_redirect( PLSpec, P, State )
	end;
handle_call( {set_pl, PLSpec = #pipeline_spec{ name = PName }}, _GenReplyTo, State = #s{} ) ->
	case erlang:get( pipeline_key( PName ) ) of
		undefined -> set_pl_create_real( PLSpec, State );
		P = #pipeline{ spec = #pipeline_redirect_spec{} } -> set_pl_replace_redirect_with_real( PLSpec, P, State );
		P = #pipeline{ spec = #pipeline_spec{} } -> set_pl_update_real( PLSpec, P, State )
	end;
handle_call( {unset_pl, PName}, _GenReplyTo, State = #s{ sup = Sup, pipelines = Pipelines0 } ) ->
	case erlang:erase( pipeline_key( PName ) ) of
		#pipeline{ spec = #pipeline_spec{}, lb_pid = LbPid } ->
			Pipelines1 = lists:keydelete( PName, #pipeline.name, Pipelines0 ),
			_ = supervisor:terminate_child( Sup, LbPid ),
			{reply, ok, State #s{ pipelines = Pipelines1 } };
		#pipeline{ spec = #pipeline_redirect_spec{} } ->
			Pipelines1 = lists:keydelete( PName, #pipeline.name, Pipelines0 ),
			{reply, ok, State #s{ pipelines = Pipelines1 }};
		undefined ->
			{reply, {error, not_found}, State}
	end;

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
				normal -> {noreply, State #s{ pipelines = Pipelines1 }};
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
pd_pipeline_store( Pipeline = #pipeline{ name = PName, lb_pid = LbPid, spec = #pipeline_spec{} } ) when is_pid( LbPid ) ->
	MonRef = erlang:monitor( process, LbPid ),
	undefined = erlang:put(
		pipeline_key( PName ),
		Pipeline #pipeline{ lb_mon_ref = MonRef } ),
	ok;
pd_pipeline_store( Pipeline = #pipeline{ name = PName, spec = #pipeline_redirect_spec{} } ) ->
	undefined = erlang:put(
		pipeline_key( PName ), Pipeline ),
	ok.

-spec pd_pipeline_erase( PName :: pipe_name() ) -> ok.
pd_pipeline_erase( PName ) ->
	#pipeline{} = erlang:erase( pipeline_key( PName ) ),
	ok.

-spec pipeline_key( PName :: pipe_name() ) -> term().
pipeline_key( PName ) -> [ pipeline | PName ].

-spec init_pipeline_fun( Sup :: pid() ) -> fun( ( pipeline_spec() ) -> #pipeline{} ).
init_pipeline_fun( Sup ) ->
	fun
		(PSpec = #pipeline_spec{ name = PName } ) ->
			{ok, LbPid} = supervisor:start_child( Sup, [ PName ] ),
			#pipeline{ name = PName, spec = PSpec, lb_pid = LbPid };
		(PSpec = #pipeline_redirect_spec{ name = PName }) ->
			#pipeline{ name = PName, spec = PSpec }
	end.
	
set_pl_update_real( PLSpec = #pipeline_spec{ name = PName }, P0, State = #s{ pipelines = Pipelines0 } ) ->
	P1 = P0 #pipeline{ spec = PLSpec },
	_ = erlang:put( pipeline_key( PName ), P1 ),
	Pipelines1 = lists:keyreplace( PName, #pipeline.name, Pipelines0, P1 ),
	{reply, {ok, updated}, State #s{ pipelines = Pipelines1 }}.

set_pl_create_real( PLSpec = #pipeline_spec{}, State = #s{ sup = Sup, pipelines = Pipelines } ) ->
	P = (init_pipeline_fun( Sup )) ( PLSpec ),
	ok = pd_pipeline_store( P ),
	{reply, {ok, created}, State #s{ pipelines = [ P | Pipelines ] }}.

set_pl_update_redirect( PLSpec = #pipeline_redirect_spec{ name = PName }, P0, State = #s{ pipelines = Pipelines0 } ) ->
	P1 = P0 #pipeline{ spec = PLSpec },
	_ = erlang:put( pipeline_key( PName ), P1 ),
	Pipelines1 = lists:keyreplace( PName, #pipeline.name, Pipelines0, P1 ),
	{reply, {ok, updated}, State #s{ pipelines = Pipelines1 }}.

set_pl_create_redirect( PLSpec = #pipeline_redirect_spec{}, State = #s{ sup = Sup, pipelines = Pipelines } ) ->
	P = (init_pipeline_fun( Sup )) ( PLSpec ),
	ok = pd_pipeline_store( P ),
	{reply, {ok, created}, State #s{ pipelines = [ P | Pipelines ] }}.

set_pl_replace_real_with_redirect(
		PLSpec = #pipeline_redirect_spec{ name = PName },
		P0 = #pipeline{ lb_pid = LbPid, lb_mon_ref = LbMonRef },
		State = #s{ sup = Sup, pipelines = Pipelines0 }
	) ->
		_ = erlang:demonitor( LbMonRef, [flush] ),
		_ = supervisor:terminate_child( Sup, LbPid ),
		P1 = P0 #pipeline{ lb_pid = undefined, lb_mon_ref = undefined, spec = PLSpec },
		ok = pd_pipeline_erase( PName ),
		ok = pd_pipeline_store( P1 ),
		Pipelines1 = lists:keyreplace( PName, #pipeline.name, Pipelines0, P1 ),
		{reply, {ok, updated}, State #s{ pipelines = Pipelines1 } }.

set_pl_replace_redirect_with_real(
		PLSpec = #pipeline_spec{ name = PName }, 
		_P0 = #pipeline{},
		State = #s{ sup = Sup, pipelines = Pipelines0 }
	) ->
		P1 = (init_pipeline_fun(Sup)) ( PLSpec ),
		ok = pd_pipeline_erase( PName ),
		ok = pd_pipeline_store( P1 ),
		Pipelines1 = lists:keyreplace( PName, #pipeline.name, Pipelines0, P1 ),
		{reply, {ok, updated}, State #s{ pipelines = Pipelines1 }}.

