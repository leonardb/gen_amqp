%%% @copyright 2025 Leonard Boyce <leonard_at_usethesource.net>
%%% @author Leonard Boyce <leonard_at_usethesource.net>
%%% @license MIT
%%% @doc
%%% This module provides a generic AMQP client interface using the `gen_server` behavior.
%%% It allows for connection management, message publishing, consuming, and handling various AMQP events.
%%% @end
-module(gen_amqp).

-behaviour(gen_server).

-define(DEFAULT_ROUTING_KEY, <<"">>).
-define(CONN_KEY, '$gen_amqp_conn').

-include_lib("kernel/include/logger.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-type queue_name() :: binary().
-type tag() :: binary() | {binary(), binary()}.
-type consumer_tag() :: {queue_name(), tag()}.
-type amqp_connection() :: #{
    mod := module(),
    conn := pid(),
    channel := binary(),
    confirms_handler := undefined | pid() | {atom(), _} | {'via', _, _} | fun((_, _) -> any()),
    buffer := none | queue | module(),
    auto_ack_unhandled := boolean(),
    name => atom() | binary(),
    consumer_tags := [consumer_tag()]
}.
-type reply_code() :: pos_integer().
-type reply_text() :: binary().
-type exchange() :: binary().
-type routing_key() :: binary().
-type return_tag() :: {reply_code(), reply_text(), exchange(), routing_key()}.
-type message() :: any().
-type amqp_msg() :: #amqp_msg{}.
-type deliver_payload() :: {#'basic.deliver'{}, amqp_msg()}.
-type basic_cancel() :: #'basic.cancel'{}.
-type basic_reject() :: #'basic.reject'{}.
-type channel_tag() :: any().
-type name() :: {local, atom()} | {global, term()} | {via, atom(), term()}.
-type queue_options() :: [
    {expires, pos_integer()}
    | {mode, binary() | list()}
    | {durable, boolean()}
    | {auto_delete, boolean()}
    | {exclusive, boolean()}
].

-type confirms_module_function() :: {atom(), atom()}.
-type confirms_fun() :: fun((ack | nack, _) -> any()).
-type confirms_handler() ::
    'false' | 'null' | 'true' | pid() | confirms_fun() | confirms_module_function().
-type buffer_module() :: module().

%% @doc
%% <h2>Connection behavior options:</h2>
%% <ul>
%%  <li>auto_ack_unhandled: boolean for automatic acknowledgment</li>
%%  <li>buffer: buffering strategy (none, queue, or custom module)</li>
%%  <li>confirms: message confirmation handling</li>
%% </ul>
%% <h2>Connection parameters:</h2>
%%  <ul>
%%  <li>heartbeat: heartbeat interval</li>
%%  <li>host: server hostname or IP</li>
%%  <li>virtual_host: RabbitMQ virtual host</li>
%%  <li>username: authentication username</li>
%%  <li>password: authentication password</li>
%%  </ul>
%% <h2>Network settings:</h2>
%%  <ul>
%%  <li>port: server port number</li>
%%  <li>channel_max: maximum number of channels</li>
%%  <li>frame_max: maximum frame size</li>
%%  <li>connection_timeout: connection timeout</li>
%%  <li>ssl_options: SSL configuration</li>
%%  <li>auth_mechanisms: authentication mechanisms</li>
%%  <li>client_properties: client properties</li>
%%  <li>socket_options: TCP socket options</li>
%%  </ul>
%% @end
-type config() :: [
    {auto_ack_unhandled, boolean()}
    | {buffer, none | queue | buffer_module()}
    | {confirms, confirms_handler()}
    | {heartbeat, pos_integer()}
    | {host, inet:ip_address() | string()}
    | {virtual_host, binary()}
    | {username, binary()}
    | {password, binary()}
    | {port, pos_integer()}
    | {channel_max, pos_integer()}
    | {frame_max, pos_integer()}
    | {connection_timeout, pos_integer()}
    | {ssl_options, list()}
    | {auth_mechanisms, list()}
    | {client_properties, list()}
    | {socket_options, list()}
].
-type parsed_config() :: #{
    auto_ack_unhandled := boolean(),
    buffer := none | queue | buffer_module(),
    confirms := confirms_handler(),
    network := #amqp_params_network{},
    mod := module()
}.
-type args() :: term().
-type stimeout() :: pos_integer() | infinity | hibernate.
-type file() :: list().
-type sflag() :: trace | log | {logfile, file()} | statistics | debug.
-type options() :: [{timeout, stimeout()} | {debug, [sflag()]}].

-record(inner_state, {
    module :: atom(),
    status = disconnected :: connected | disconnected,
    setup_pid = undefined :: undefined | pid(),
    config :: parsed_config(),
    queue = queue:new() :: queue:queue()
}).
%% Required callbacks

%% Called when the gen_amqp process is started.
-callback init(Args :: config()) ->
    {ok, State :: term()}
    | {ok, State :: term(), timeout() | hibernate | {continue, term()}}
    | {stop, Reason :: term()}
    | ignore.

%% Called when the AMQP connection is established.
-callback handle_connect(State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when the AMQP connection is lost.
-callback handle_disconnect(Reason :: term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when a message is received by the consumer.
-callback handle_basic_deliver(
    {Tag :: tag(), Message :: message()},
    State :: term()
) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when an RPC message is received.
-callback handle_rpc(
    Message :: {invalid_term, binary()} | {message(), term()},
    State :: term()
) ->
    {reply, Reply :: term(), NewState :: term()}
    | {reply, Reply :: term(), NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when a queue is deleted.
-callback handle_queue_deleted(Queue :: queue_name(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when a consumer is cancelled.
-callback handle_basic_cancel(ChannelTag :: channel_tag(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when a message is rejected.
-callback handle_basic_reject(RejectTag :: tag(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when a message is returned.
-callback handle_basic_return(ReturnTag :: return_tag(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when an error occurs during delivery or RPC.
-callback handle_error(
    From :: basic_deliver | rpc, Reason :: atom(), {Tag :: binary(), Msg :: any()}, State :: term()
) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called for synchronous calls to the gen_server.
-callback handle_call(
    Request :: term(),
    From :: {pid(), Tag :: term()},
    State :: term()
) ->
    {reply, Reply :: term(), NewState :: term()}
    | {reply, Reply :: term(), NewState :: term(), timeout() | hibernate}
    | {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called for asynchronous casts to the gen_server.
-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called for all other messages.
-callback handle_info(Info :: timeout() | term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called for continue instructions after init.
-callback handle_continue(Info :: term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), timeout() | hibernate | {continue, term()}}
    | {stop, Reason :: term(), NewState :: term()}.

%% Called when the gen_amqp process terminates.
-callback terminate(
    Reason ::
        (normal
        | shutdown
        | {shutdown, term()}
        | term()),
    State :: term()
) ->
    term().

%% Called when the code is upgraded/downgraded.
-callback code_change(
    OldVsn :: (term() | {down, term()}),
    State :: term(),
    Extra :: term()
) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

-optional_callbacks(
    [
        handle_continue/2,
        handle_queue_deleted/2,
        handle_basic_cancel/2,
        handle_basic_reject/2,
        handle_basic_return/2,
        handle_rpc/2,
        handle_error/4
    ]
).

-export_type([
    channel_tag/0,
    return_tag/0,
    message/0
]).

%% Standard gen_server calls
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).
-export([
    start/4,
    start/5,
    start_link/4,
    start_link/5,
    abcast/2, abcast/3,
    call/2, call/3,
    cast/2,
    enter_loop/3, enter_loop/4, enter_loop/5,
    multi_call/2, multi_call/3, multi_call/4,
    reply/2
]).

%% API
-export([
    ack/1,
    nack/1,
    bind/2, bind/3,
    connect/1, connect/2,
    connection_pid/1,
    disconnect/0,
    direct_exchange/1,
    fanout_exchange/1,
    publish/2, publish/3,
    publish_confirmed/2, publish_confirmed/3,
    publish_confirmed_async/4,
    wait_for_confirms/1, wait_for_confirms/2,
    rpc_request/2,
    rpc_response/3,
    queue/1, queue/2,
    queue_length/1,
    subscribe/1, subscribe/2, subscribe/3,
    unsubscribe/1,
    unsubscribe_all/0,
    topic_exchange/1
]).

-export([
    payload/1,
    tag/1
]).

init({Module, Config0, Args}) ->
    ?LOG_INFO("Init ~p with Config ~p and Args ~p", [Module, Config0, Args]),
    {ok, Config} = parse_config(Module, Config0),
    ?LOG_INFO("Config: ~p", [Config]),
    wrap_result(
        setup_conn(#inner_state{module = Module, config = Config}), Module:init(Args)
    ).

handle_call(Request, From, {#inner_state{module = Module} = InnerState, State}) ->
    wrap_result(InnerState, Module:handle_call(Request, From, State)).

handle_cast({buff_write, BinTerm}, {#inner_state{queue = Queue} = InnerState, State}) ->
    {noreply, {InnerState#inner_state{queue = queue:in(BinTerm, Queue)}, State}};
handle_cast(Request, {#inner_state{module = Module} = InnerState, State}) ->
    wrap_result(InnerState, Module:handle_cast(Request, State)).

handle_continue(Request, {#inner_state{module = Module} = InnerState, State}) ->
    case erlang:function_exported(Module, handle_continue, 2) of
        true ->
            wrap_result(InnerState, Module:handle_continue(Request, State));
        false ->
            {noreply, {InnerState, State}}
    end.

handle_info(#'basic.cancel'{consumer_tag = Tag}, {
    #inner_state{module = Module} = InnerState,
    State
}) ->
    #{auto_ack_unhandled := AutoAckUnhandled, consumer_tags := ConsumerTags} =
        Conn = get(?CONN_KEY),
    case lists:keytake(Tag, 2, ConsumerTags) of
        false ->
            case erlang:function_exported(Module, handle_basic_cancel, 2) of
                true ->
                    wrap_result(InnerState, Module:handle_basic_cancel(Tag, State));
                false when AutoAckUnhandled ->
                    ?LOG_INFO("Auto-ack for missing handle_basic_cancel/2: ~p ~p ~p", [
                        Tag, InnerState, State
                    ]),
                    ack_(Conn, Tag),
                    wrap_result(InnerState, State);
                false ->
                    ?LOG_INFO("Missing handle_basic_cancel/2: ~p", [Tag]),
                    wrap_result(InnerState, State)
            end;
        {value, {Queue, Tag}, NewConsumerTags} ->
            ?LOG_INFO("Consumer tag ~p removed from ~p", [Tag, Queue]),
            %% ack(Conn, Tag),
            put(?CONN_KEY, Conn#{consumer_tags => NewConsumerTags}),
            case erlang:function_exported(Module, handle_queue_deleted, 2) of
                true ->
                    wrap_result(InnerState, Module:handle_queue_deleted(Queue, State));
                false when AutoAckUnhandled ->
                    ?LOG_INFO("Auto-ack for missing handle_queue_deleted/2: ~p ~p ~p", [
                        Tag, InnerState, State
                    ]),
                    wrap_result(InnerState, State);
                false ->
                    ?LOG_INFO("Missing handle_queue_deleted/2: ~p", [Tag]),
                    wrap_result(InnerState, State)
            end
    end;
handle_info(#'basic.reject'{delivery_tag = DeliveryTag}, {
    #inner_state{module = Module} = InnerState,
    State
}) ->
    #{auto_ack_unhandled := AutoAckUnhandled} = Conn = get(?CONN_KEY),
    case erlang:function_exported(Module, handle_basic_reject, 2) of
        true ->
            wrap_result(InnerState, Module:handle_basic_reject(DeliveryTag, State));
        false when AutoAckUnhandled ->
            ?LOG_INFO("Auto-ack for missing handle_basic_reject/2: ~p", [DeliveryTag]),
            ack_(Conn, DeliveryTag),
            wrap_result(InnerState, State);
        false ->
            ?LOG_INFO("Missing handle_basic_reject/2: ~p", [DeliveryTag]),
            wrap_result(InnerState, State)
    end;
handle_info(
    #'basic.return'{
        reply_code = ReplyCode,
        reply_text = ReplyText,
        exchange = Exchange,
        routing_key = RoutingKey
    },
    {#inner_state{module = Module} = InnerState, State}
) ->
    case erlang:function_exported(Module, handle_basic_return, 2) of
        true ->
            wrap_result(
                InnerState,
                Module:handle_basic_return({ReplyCode, ReplyText, Exchange, RoutingKey}, State)
            );
        false ->
            ?LOG_INFO("Missing handle_basic_return/2: ~p", [
                {ReplyCode, ReplyText, Exchange, RoutingKey}
            ]),
            wrap_result(InnerState, State)
    end;
handle_info(
    {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{
        payload = PayloadBin,
        props = #'P_basic'{
            correlation_id = CorrelationId,
            reply_to = ReplyQ
        }
    }} = InPayload,
    {
        #inner_state{module = Module} = InnerState,
        State
    }
) when ReplyQ =/= undefined andalso CorrelationId =/= undefined ->
    #{auto_ack_unhandled := AutoAckUnhandled} = Conn = get(?CONN_KEY),
    case erlang:function_exported(Module, handle_rpc, 2) of
        true ->
            try decode(PayloadBin) of
                Term ->
                    case Module:handle_rpc({Tag, Term}, State) of
                        {reply, Reply, NState} ->
                            ok = rpc_response(Conn, InPayload, Reply),
                            wrap_result(InnerState, {noreply, NState});
                        {reply, Reply, NState, Timeout} ->
                            ok = rpc_response(Conn, InPayload, Reply),
                            wrap_result(InnerState, {noreply, NState, Timeout});
                        Other ->
                            wrap_result(InnerState, Other)
                    end
            catch
                _E:_M:_St ->
                    case erlang:function_exported(Module, handle_error, 5) of
                        true ->
                            wrap_result(
                                InnerState,
                                Module:handle_error(rpc, invalid_term, {Tag, PayloadBin}, State)
                            );
                        false when AutoAckUnhandled ->
                            ?LOG_ERROR(
                                "Auto-ack Invalid RPC payload. Tag: ~p CorrelationId: ~p ReplyQ: ~p Payload: ~p",
                                [
                                    Tag, CorrelationId, ReplyQ, PayloadBin
                                ]
                            ),
                            ack_(Conn, Tag),
                            wrap_result(InnerState, State);
                        false ->
                            ?LOG_ERROR(
                                "Invalid RPC payload. Tag: ~p CorrelationId: ~p ReplyQ: ~p Payload: ~p",
                                [
                                    Tag, CorrelationId, ReplyQ, PayloadBin
                                ]
                            ),
                            wrap_result(InnerState, State)
                    end
            end;
        false when AutoAckUnhandled ->
            ?LOG_ERROR("Auto-ack for missing handle_rpc/2: ~p", [{Tag, PayloadBin}]),
            ack_(Conn, Tag),
            wrap_result(InnerState, State);
        false ->
            ?LOG_ERROR("Missing handle_rpc/2: ~p", [{Tag, PayloadBin}]),
            wrap_result(InnerState, State)
    end;
handle_info(
    {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{
        payload = PayloadBin,
        props = #'P_basic'{}
    }},
    {
        #inner_state{module = Module} = InnerState,
        State
    }
) ->
    #{auto_ack_unhandled := AutoAckUnhandled} = Conn = get(?CONN_KEY),
    try decode(PayloadBin) of
        Term ->
            wrap_result(InnerState, Module:handle_basic_deliver({Tag, Term}, State))
    catch
        _E:_M:_St ->
            case erlang:function_exported(Module, handle_error, 4) of
                true ->
                    wrap_result(
                        InnerState,
                        Module:handle_error(basic_deliver, invalid_term, {Tag, PayloadBin}, State)
                    );
                false when AutoAckUnhandled ->
                    ?LOG_ERROR(
                        "Auto-ack for invalid basic deliver payload ~p: ~p",
                        [Tag, PayloadBin]
                    ),
                    ack_(Conn, Tag),
                    wrap_result(InnerState, State);
                false ->
                    ?LOG_ERROR(
                        "Invalid basic deliver payload ~p: ~p",
                        [Tag, PayloadBin]
                    ),
                    wrap_result(InnerState, State)
            end
    end;
handle_info({'EXIT', SetupPid, {normal, NewConn0}}, {
    #inner_state{setup_pid = SetupPid, module = Module} = InnerState0,
    State
}) ->
    ?LOG_INFO("New conn: ~p", [NewConn0]),
    NewConn = NewConn0#{mod => Module},
    AmqpPid = connection_pid(NewConn),
    link(AmqpPid),
    put(?CONN_KEY, NewConn),
    InnerState1 = InnerState0#inner_state{
        status = connected, setup_pid = undefined
    },
    ?LOG_INFO("Processing any buffered messages..."),
    {ok, InnerState} = process_buffered_messages(InnerState1, NewConn),
    ?LOG_INFO("Completed processing of buffered messages... ~p", [InnerState]),
    wrap_result(InnerState, Module:handle_connect(State));
handle_info({'EXIT', CrashedPid, Reason}, {
    #inner_state{module = Module} = InnerState0, State
}) ->
    #{conn := ConnPid} = get(?CONN_KEY),
    case CrashedPid of
        ConnPid ->
            ?LOG_ERROR("Connection crashed: ~p", [Reason]),
            InnerState = setup_conn(InnerState0#inner_state{status = disconnected}),
            wrap_result(InnerState, Module:handle_disconnect(Reason, State));
        _ ->
            ?LOG_ERROR("Unexpected exit from ~p: ~p", [CrashedPid, Reason]),
            wrap_result(InnerState0, State)
    end;
% InnerState = setup_conn(InnerState0#inner_state{conn = undefined, status = disconnected}),
% wrap_result(InnerState, Module:handle_disconnect(Reason, State));
handle_info(Message, {#inner_state{module = Module} = InnerState, State}) ->
    wrap_result(InnerState, Module:handle_info(Message, State)).

terminate(Reason, {#inner_state{module = Module} = _InnerState, State}) ->
    Module:terminate(Reason, State),
    case get(?CONN_KEY) of
        undefined ->
            ?LOG_INFO("No connection to terminate, Reason: ~p", [Reason]);
        Conn ->
            ?LOG_INFO("Terminating connection: ~p, Reason: ~p", [Conn, Reason]),
            disconnect_(Conn)
    end.

code_change(OldVsn, {#inner_state{module = Module} = InnerState, State}, Extra) ->
    case Module:code_change(OldVsn, State, Extra) of
        {ok, NewState} ->
            {ok, {InnerState, NewState}};
        {error, Reason} ->
            {error, Reason}
    end.

%% gen_server wrapping
-spec start(Module :: module(), Config :: config(), Args :: args(), Options :: options()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start(Module, Config, Args, Options) ->
    gen_server:start(?MODULE, {Module, Config, Args}, Options).

-spec start(
    ServerName :: name(),
    Module :: module(),
    Config :: config(),
    Args :: args(),
    Options :: options()
) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start(ServerName, Module, Config, Args, Options) ->
    gen_server:start(ServerName, ?MODULE, {Module, Config, Args}, Options).

-spec start_link(Module :: module(), Config :: config(), Args :: args(), Options :: options()) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start_link(Module, Config, Args, Options) ->
    ?LOG_INFO("Starting ~p with Config ~p and Args ~p", [Module, Config, Args]),
    gen_server:start_link(?MODULE, {Module, Config, Args}, Options).

-spec start_link(
    ServerName :: name(),
    Module :: module(),
    Config :: config(),
    Args :: args(),
    Options :: options()
) ->
    {ok, pid()} | {error, {already_started, pid()}} | {error, term()}.
start_link(ServerName, Module, Config, Args, Options) ->
    ?LOG_INFO("Starting ~p ~p with Config ~p and Args ~p", [ServerName, Module, Config, Args]),
    gen_server:start_link(ServerName, ?MODULE, {Module, Config, Args}, Options).

abcast(X, Y) ->
    gen_server:abcast(X, Y).

abcast(X, Y, Z) ->
    gen_server:abcast(X, Y, Z).

call(X, Y) ->
    gen_server:call(X, Y).

call(X, Y, Z) ->
    gen_server:call(X, Y, Z).

multi_call(X, Y) ->
    gen_server:multi_call(X, Y).

multi_call(X, Y, Z) ->
    gen_server:multi_call(X, Y, Z).

multi_call(A, B, C, D) ->
    gen_server:multi_call(A, B, C, D).

cast(X, Y) ->
    gen_server:cast(X, Y).

enter_loop(X, Y, Z) ->
    gen_server:enter_loop(X, Y, Z).

enter_loop(X, Y, Z, A) ->
    gen_server:enter_loop(X, Y, Z, A).

enter_loop(X, Y, Z, A, B) ->
    gen_server:enter_loop(X, Y, Z, A, B).

reply(A, B) ->
    gen_server:reply(A, B).

wrap_result(_InnerState, ignore) ->
    ignore;
wrap_result(InnerState, {ok, State}) ->
    {ok, {InnerState, State}};
wrap_result(InnerState, {ok, State, {continue, Term}}) ->
    {ok, {InnerState, State}, {continue, Term}};
wrap_result(InnerState, {ok, State, Timeout}) ->
    {ok, {InnerState, State}, Timeout};
wrap_result(_InnerState, {stop, Reason}) ->
    {stop, Reason};
wrap_result(InnerState, {stop, Reason, State}) ->
    {stop, Reason, {InnerState, State}};
wrap_result(InnerState, {stop, Reason, Reply, State}) ->
    {stop, Reason, Reply, {InnerState, State}};
wrap_result(InnerState, {reply, Reply, State}) ->
    {reply, Reply, {InnerState, State}};
wrap_result(InnerState, {reply, Reply, State, Timeout}) ->
    {reply, Reply, {InnerState, State}, Timeout};
wrap_result(InnerState, {noreply, State}) ->
    {noreply, {InnerState, State}};
wrap_result(InnerState, {noreply, State, Timeout}) ->
    {noreply, {InnerState, State}, Timeout}.

setup_conn(#inner_state{config = Config} = State) ->
    Pid = spawn_link(fun() ->
        {ok, Conn} = setup_amqp_conn(Config),
        exit({normal, Conn})
    end),
    State#inner_state{
        setup_pid = Pid, status = disconnected
    }.

setup_amqp_conn(Config) ->
    setup_amqp_conn(Config, self()).
setup_amqp_conn(Config, Pid) ->
    setup_amqp_conn(Config, Pid, 0).

setup_amqp_conn(Config, Pid, Retries) ->
    case connect(Config, Pid) of
        {ok, Conn} ->
            ?LOG_INFO("AMQP connection established successfully."),
            {ok, Conn};
        {error, Reason} ->
            ?LOG_ERROR("AMQP connection failed: ~p", [Reason]),
            retry_connection(Config, Pid, Retries + 1)
    end.

retry_connection(Config, Pid, Retries) ->
    Wait = backoff(60, Retries),
    ?LOG_WARNING("Retrying AMQP connection in ~p ms... (~p attempts)", [Wait, Retries]),
    timer:sleep(Wait),
    setup_amqp_conn(Config, Pid, Retries).

%% @doc Backoff using Fibonacci sequence to Max seconds
-spec backoff(non_neg_integer(), non_neg_integer()) -> pos_integer().
backoff(Max, N) when
    is_integer(Max) andalso Max >= 0 andalso is_integer(N) andalso N >= 0 andalso N =< Max
->
    MaxMs = Max * 1000,
    case backoff(N, 0, 1) of
        BackoffMs when BackoffMs > MaxMs ->
            MaxMs;
        BackoffMs ->
            BackoffMs
    end;
backoff(Max, N) when is_integer(Max) andalso Max >= 0 andalso is_integer(N) andalso N > Max ->
    Max * 1000.

backoff(0, _, R) ->
    R * 1000;
backoff(N, B1, B2) ->
    backoff(N - 1, B2, B1 + B2).

% process_buffered_messages(#inner_state{conn = undefined} = State) ->
%     {ok, State};
-spec process_buffered_messages(
    State :: #inner_state{},
    amqp_connection()
) -> {ok, #inner_state{}}.
process_buffered_messages(#inner_state{} = State, #{buffer := none}) ->
    {ok, State};
process_buffered_messages(
    #inner_state{queue = Queue} = State,
    #{buffer := queue} = Conn
) ->
    case queue:out(Queue) of
        {{value, Message0}, NQueue} ->
            {Exchange, RoutingKey, Payload0} = binary_to_term(Message0),
            try
                publish_confirmed_(
                    Conn,
                    Exchange,
                    RoutingKey,
                    Payload0
                )
            of
                ok ->
                    process_buffered_messages(State#inner_state{queue = NQueue}, Conn);
                {ok, buffered} ->
                    {ok, State#inner_state{queue = NQueue}};
                {error, _} = Error ->
                    ?LOG_ERROR("Failed to send message: ~p", [Error]),
                    {ok, State}
            catch
                E:M ->
                    ?LOG_ERROR("Failed to send message: ~p:~p", [E, M]),
                    {ok, State}
            end;
        {empty, _} ->
            {ok, State}
    end;
process_buffered_messages(
    #inner_state{config = Config} = State,
    #{mod := ImplMod, buffer := Mod}
) ->
    ?LOG_INFO("Creating new connection for buffered messages... ~p", [State]),
    Name = list_to_atom(lists:flatten(io_lib:format("~p_~p", [unbuffer, ImplMod]))),
    {ok, UnbufferConn} = connect(
        Config#{
            confirms => unbuffer_confirms_fun(ImplMod, Mod)
        },
        Name
    ),
    ?LOG_INFO("Unbuffer conn: ~p", [UnbufferConn]),
    ok = unbuffer_loop(State, UnbufferConn),
    {ok, State}.

unbuffer_loop(#inner_state{} = State, #{mod := ImplMod, buffer := Mod} = Conn) ->
    case Mod:buff_lock(ImplMod, 1) of
        {ok, []} ->
            disconnect_(Conn),
            ok;
        {ok, Messages0} when is_list(Messages0) ->
            {ok, _Ids} = process_buffered_messages_(Messages0, Conn, []),
            wait_for_confirms(Conn),
            unbuffer_loop(State, Conn);
        {error, disconnected, Ids} ->
            [Mod:buff_unlock(Id) || Id <- Ids],
            ?LOG_ERROR("Unbuffer loop failed: ~p", [Ids]),
            disconnect_(Conn),
            ok
    end.

-spec process_buffered_messages_(
    Messages :: [{binary(), binary()}],
    Conn :: amqp_connection(),
    Ids :: [binary()]
) -> {ok, [binary()]}.
process_buffered_messages_([], _Conn, Ids) ->
    {ok, Ids};
process_buffered_messages_(
    [{Id, Message0} | Rest], Conn, Ids
) ->
    {Exchange, RoutingKey, Payload0} = binary_to_term(Message0),
    try
        publish_confirmed_async_(
            Conn,
            Exchange,
            RoutingKey,
            Id,
            Payload0
        )
    of
        ok ->
            process_buffered_messages_(Rest, Conn, [Id | Ids]);
        {error, ErrAtom} = Error ->
            ?LOG_ERROR("Failed to send message: ~p", [Error]),
            {error, ErrAtom, Ids}
    catch
        E:M ->
            ?LOG_ERROR("Failed to send message: ~p:~p", [E, M]),
            {ok, E, Ids}
    end.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc acknowledge a message
-spec ack(deliver_payload() | basic_cancel() | binary()) -> ok.
ack(M) ->
    Conn = get(?CONN_KEY),
    ack_(Conn, M).

-spec ack_(Conn :: amqp_connection(), deliver_payload() | basic_cancel() | binary()) -> ok.
ack_(#{channel := Channel}, {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{}}) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag});
ack_(#{channel := Channel}, #'basic.cancel'{consumer_tag = Tag}) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag});
ack_(#{channel := Channel}, Tag) ->
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}).

%% @doc negative acknowledge a message
-spec nack(deliver_payload() | basic_cancel() | binary()) -> ok.
nack(M) ->
    Conn = get(?CONN_KEY),
    nack_(Conn, M).

-spec nack_(Conn :: amqp_connection(), deliver_payload() | basic_cancel() | binary()) -> ok.
nack_(#{channel := Channel}, {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{}}) ->
    amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag});
nack_(#{channel := Channel}, #'basic.cancel'{consumer_tag = Tag}) ->
    amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag});
nack_(#{channel := Channel}, Tag) ->
    amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag}).

%% @doc Bind a queue to an exchange with the default routing key.
-spec bind(
    Queue :: binary(),
    Exchange :: binary()
) -> ok.
bind(Queue, Exchange) ->
    Conn = get(?CONN_KEY),
    bind_(Conn, Queue, Exchange, ?DEFAULT_ROUTING_KEY).

-spec bind(
    Queue :: binary(),
    Exchange :: binary(),
    RoutingKey :: binary()
) -> ok.
bind(Queue, Exchange, RoutingKey) ->
    Conn = get(?CONN_KEY),
    bind_(Conn, Queue, Exchange, RoutingKey).

%% @doc Bind a queue to an exchange with a routing key.
-spec bind_(
    Conn :: amqp_connection(),
    Queue :: binary(),
    Exchange :: binary(),
    RoutingKey :: binary()
) -> ok.
bind_(#{channel := Channel}, Queue, Exchange, RoutingKey) ->
    Binding = #'queue.bind'{
        queue = Queue,
        exchange = Exchange,
        routing_key = RoutingKey
    },
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
    ok.

-spec connect(parsed_config()) -> {ok, amqp_connection()} | {error, term()}.
connect(#{mod := ImplMod} = Opts) ->
    connect(Opts, ImplMod, self()).

-spec connect(parsed_config(), atom() | pid()) -> {ok, amqp_connection()} | {error, term()}.
connect(#{mod := ImplMod} = Opts, Pid) when is_pid(Pid) ->
    connect(Opts, ImplMod, Pid);
connect(#{} = Opts, Name) ->
    connect(Opts, Name, self()).

-spec connect(parsed_config(), atom(), pid()) -> {ok, amqp_connection()} | {error, term()}.
connect(
    #{
        network := Params,
        confirms := Confirms,
        buffer := Buffer,
        auto_ack_unhandled := AutoAckUnhandled,
        mod := ImplMod
    },
    Name,
    ConnPid
) ->
    case amqp_connection:start(Params, atom_to_binary(Name)) of
        {ok, Connection} ->
            ?LOG_DEBUG("Connected with: ~p", [Params]),
            {ok, Channel} = amqp_connection:open_channel(Connection),
            ok = amqp_selective_consumer:register_default_consumer(Channel, ConnPid),
            HB = amqp_connection:info(Connection, [heartbeat]),
            ?LOG_INFO("Conn HB: ~p", [HB]),
            Conn = #{
                conn => Connection,
                channel => Channel,
                confirms_handler => undefined,
                buffer => Buffer,
                auto_ack_unhandled => AutoAckUnhandled,
                mod => ImplMod,
                name => Name,
                consumer_tags => []
            },
            case Confirms of
                null ->
                    {ok, Conn};
                false ->
                    {ok, Conn};
                true ->
                    #'confirm.select_ok'{} = amqp_channel:call(
                        Channel, #'confirm.select'{}
                    ),
                    {ok, Conn};
                Pid when is_pid(Pid) ->
                    #'confirm.select_ok'{} = amqp_channel:call(
                        Channel, #'confirm.select'{}
                    ),
                    amqp_channel:register_confirm_handler(Channel, Pid),
                    {ok, Conn};
                Fun when is_function(Fun, 2) ->
                    %% fun(ack | nack, any())
                    #'confirm.select_ok'{} = amqp_channel:call(
                        Channel, #'confirm.select'{}
                    ),
                    {ok, ConfirmsHandler} = amqp_confirms_handler:register(
                        Connection, Channel, Fun
                    ),
                    {ok, Conn#{confirms_handler => ConfirmsHandler}}
            end;
        {error, _} = Error ->
            ?LOG_ERROR("Failed to connect error: ~p, settings: ~p", [Error, Params]),
            Error
    end.

connection_pid(#{conn := ConnPid}) ->
    ConnPid.

parse_config(ImplMod, Opts) ->
    parse_conn_opts(Opts, #{
        network => #amqp_params_network{heartbeat = 30},
        confirms => null,
        auto_ack_unhandled => true,
        buffer => none,
        mod => ImplMod
    }).

parse_conn_opts([], Params) ->
    {ok, Params};
parse_conn_opts([{auto_ack_unhandled, true} | Opts], Params) ->
    parse_conn_opts(Opts, Params#{auto_ack_unhandled => true});
parse_conn_opts([{auto_ack_unhandled, false} | Opts], Params) ->
    parse_conn_opts(Opts, Params#{auto_ack_unhandled => false});
parse_conn_opts([{buffer, none} | Opts], Params) ->
    parse_conn_opts(Opts, Params#{buffer => none});
parse_conn_opts([{buffer, queue} | Opts], Params) ->
    parse_conn_opts(Opts, Params#{buffer => queue});
parse_conn_opts([{buffer, Mod} | Opts], Params) when is_atom(Mod) ->
    {module, Mod} = c:l(Mod),
    case
        lists:all(
            fun({Fun, Arity}) ->
                erlang:function_exported(Mod, Fun, Arity)
            end,
            [{buff_lock, 2}, {buff_unlock, 1}, {buff_put, 2}, {buff_delete, 1}]
        )
    of
        true ->
            parse_conn_opts(Opts, Params#{buffer => Mod});
        false ->
            {error, {buffer_functions_missing, Mod}}
    end;
parse_conn_opts([{confirms, true} | Opts], Params) ->
    parse_conn_opts(Opts, Params#{confirms => true});
parse_conn_opts([{confirms, false} | Opts], Params) ->
    parse_conn_opts(Opts, Params#{confirms => false});
parse_conn_opts([{confirms, Pid} | Opts], Params) when is_pid(Pid) ->
    parse_conn_opts(Opts, Params#{confirms => Pid});
parse_conn_opts([{confirms, Fun} | Opts], Params) when is_function(Fun, 2) ->
    parse_conn_opts(Opts, Params#{confirms => Fun});
parse_conn_opts([{confirms, {Mod, Fun}} | Opts], Params) when is_atom(Mod) andalso is_atom(Fun) ->
    {module, Mod} = c:l(Mod),
    case erlang:function_exported(Mod, Fun, 2) of
        true ->
            parse_conn_opts(Opts, Params#{confirms => {Mod, Fun}});
        false ->
            {error, {confirms_function_missing, {Mod, Fun}}}
    end;
parse_conn_opts([{host, Value} | Opts], #{network := Network} = Params) when is_list(Value) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{host = Value}});
parse_conn_opts([{host, {_, _, _, _} = Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{host = Value}});
parse_conn_opts([{virtual_host, Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{virtual_host = Value}});
parse_conn_opts([{username, Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{username = Value}});
parse_conn_opts([{password, Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{password = Value}});
parse_conn_opts([{port, Value} | Opts], #{network := Network} = Params) when is_integer(Value) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{port = Value}});
parse_conn_opts([{channel_max, Value} | Opts], #{network := Network} = Params) when
    is_integer(Value)
->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{channel_max = Value}});
parse_conn_opts([{frame_max, Value} | Opts], #{network := Network} = Params) when
    is_integer(Value)
->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{frame_max = Value}});
parse_conn_opts([{heartbeat, Value} | Opts], #{network := Network} = Params) when
    is_integer(Value)
->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{heartbeat = Value}});
parse_conn_opts([{connection_timeout, Value} | Opts], #{network := Network} = Params) when
    is_integer(Value)
->
    parse_conn_opts(Opts, Params#{
        network => Network#amqp_params_network{connection_timeout = Value}
    });
parse_conn_opts([{ssl_options, Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{ssl_options = Value}});
parse_conn_opts([{auth_mechanisms, Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{auth_mechanisms = Value}});
parse_conn_opts([{client_properties, Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{client_properties = Value}});
parse_conn_opts([{socket_options, Value} | Opts], #{network := Network} = Params) ->
    parse_conn_opts(Opts, Params#{network => Network#amqp_params_network{socket_options = Value}}).

%% @doc Disconnect from the AMQP server.
disconnect() ->
    Conn = get(?CONN_KEY),
    disconnect_(Conn).

-spec disconnect_(Conn :: amqp_connection()) -> ok.
disconnect_(#{channel := Channel, conn := Connection, confirms_handler := _ConfirmsHandler} = Conn) ->
    ?LOG_INFO("Disconnecting... ~p", [Conn]),
    catch amqp_confirms_handler:unregister(_ConfirmsHandler),
    catch amqp_channel:close(Channel),
    catch amqp_connection:close(Connection),
    ok.

%% @doc Create a direct exchange
-spec direct_exchange(ExchangeName :: binary()) -> ok.
direct_exchange(ExchangeName) ->
    Conn = get(?CONN_KEY),
    direct_exchange_(Conn, ExchangeName).

-spec direct_exchange_(Conn :: amqp_connection(), ExchangeName :: binary()) -> ok.
direct_exchange_(#{channel := Channel}, ExchangeName) ->
    Declare = #'exchange.declare'{exchange = ExchangeName, durable = true},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
    ok.

%% @doc Create a fanout exchange
-spec fanout_exchange(ExchangeName :: binary()) -> ok.
fanout_exchange(ExchangeName) ->
    Conn = get(?CONN_KEY),
    fanout_exchange_(Conn, ExchangeName).

-spec fanout_exchange_(Conn :: amqp_connection(), ExchangeName :: binary()) -> ok.
fanout_exchange_(#{channel := Channel}, ExchangeName) ->
    Declare = #'exchange.declare'{
        exchange = ExchangeName,
        type = <<"fanout">>,
        durable = true
    },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
    ok.

%% @doc Create a topic exchange
-spec topic_exchange(ExchangeName :: binary()) -> ok.
topic_exchange(ExchangeName) ->
    Conn = get(?CONN_KEY),
    topic_exchange_(Conn, ExchangeName).

topic_exchange_(#{channel := Channel}, ExchangeName) ->
    Declare = #'exchange.declare'{
        exchange = ExchangeName,
        type = <<"topic">>,
        durable = true
    },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),
    ok.

handle_failure(Buffer, Mod, What) ->
    case Buffer of
        none ->
            ?LOG_ERROR("connection dead. buffer type: ~p", [Buffer]),
            {error, disconnected};
        queue ->
            ?LOG_ERROR("connection dead. buffer type: ~p", [Buffer]),
            gen_server:cast(
                self(), {buff_write, term_to_binary(What)}
            ),
            {ok, buffered};
        BufferMod ->
            case BufferMod:buff_put(Mod, term_to_binary(What)) of
                ok ->
                    ?LOG_ERROR("connection dead, buffer success buffer type: ~p", [BufferMod]),
                    {ok, buffered};
                Error ->
                    ?LOG_ERROR("Failed buffering: ~p ~p:buff_write/2", [Error, BufferMod]),
                    {error, buffer_failed}
            end
    end.

%% @doc Publish a message to an exchange with the default routing key.
-spec publish(binary(), term()) ->
    ok | {ok, buffered} | {error, disconnected | buffer_failed}.
publish(Exchange, Payload) ->
    Conn = get(?CONN_KEY),
    publish_(Conn, Exchange, ?DEFAULT_ROUTING_KEY, Payload).

%% @doc Publish a message to an exchange with a routing key.
-spec publish(binary(), binary(), term()) ->
    ok | {ok, buffered} | {error, disconnected | buffer_failed}.
publish(Exchange, RoutingKey, Payload) ->
    Conn = get(?CONN_KEY),
    publish_(Conn, Exchange, RoutingKey, Payload).

-spec publish_(amqp_connection(), binary(), binary(), term()) ->
    ok | {ok, buffered} | {error, disconnected | buffer_failed}.
publish_(#{mod := Mod, channel := Channel, buffer := Buffer}, Exchange, RoutingKey, Payload0) ->
    Payload = encode(Payload0),
    try
        Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
        Props = #'P_basic'{delivery_mode = 2},
        Msg = #amqp_msg{props = Props, payload = Payload},
        amqp_channel:call(Channel, Publish, Msg)
    catch
        exit:{noproc, {gen_server, call, _}} ->
            %% amqp connection is down
            handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0});
        shutdown:_ ->
            handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0})
    end.

%% @doc Get the length of a queue. This number can be off by 1 if the connection is consuming from the queue.
queue_length(Queue) ->
    Conn = get(?CONN_KEY),
    queue_length_(Conn, Queue).

-spec queue_length_(amqp_connection(), binary()) -> {ok, non_neg_integer()} | {error, term()}.
queue_length_(#{channel := Channel} = _AmqpConn, Queue) ->
    try amqp_channel:call(Channel, #'basic.get'{queue = Queue}) of
        #'basic.get_empty'{} ->
            {ok, 0};
        {#'basic.get_ok'{delivery_tag = Tag, message_count = MessageCount}, _Content} ->
            %% nack the payload since we just wanted the count
            amqp_channel:call(Channel, #'basic.nack'{delivery_tag = Tag}),
            %% since we're nacking, add 1 to the count
            {ok, MessageCount + 1}
    catch
        exit:{{shutdown, {server_initiated_close, 404, _}}, _} ->
            {error, no_such_queue};
        exit:{noproc, {gen_server, call, _}} ->
            {error, disconnected}
    end.

%% @doc Publish a message and wait for confirmation.
-spec publish_confirmed(binary(), term()) ->
    ok | {ok, buffered} | {error, disconnected | buffer_failed}.
publish_confirmed(Exchange, Payload) ->
    Conn = get(?CONN_KEY),
    publish_confirmed_(Conn, Exchange, ?DEFAULT_ROUTING_KEY, Payload).

-spec publish_confirmed(binary(), binary(), term()) ->
    ok | {ok, buffered} | {error, disconnected | buffer_failed}.
publish_confirmed(Exchange, RoutingKey, Payload) ->
    Conn = get(?CONN_KEY),
    publish_confirmed_(Conn, Exchange, RoutingKey, Payload).

-spec publish_confirmed_(
    amqp_connection(),
    binary(),
    binary(),
    term()
) -> ok | {ok, buffered} | {error, disconnected | buffer_failed}.
publish_confirmed_(
    #{mod := Mod, channel := Channel, buffer := Buffer}, Exchange, RoutingKey, Payload0
) ->
    Payload = encode(Payload0),
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Props = #'P_basic'{delivery_mode = 2},
    Msg = #amqp_msg{props = Props, payload = Payload},
    try amqp_channel:call(Channel, Publish, Msg) of
        ok ->
            try
                true = amqp_channel:wait_for_confirms(Channel),
                ok
            catch
                _E:_M ->
                    ?LOG_ERROR("Confirms handler failure: ~p ~p", [_E, _M]),
                    handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0})
            end;
        blocked ->
            handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0});
        closing ->
            handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0})
    catch
        exit:{noproc, {gen_server, call, _}} ->
            %% amqp connection is down
            handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0});
        shutdown:_ ->
            handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0});
        E:M ->
            ?LOG_ERROR("Failed to publish: ~p ~p", [E, M]),
            handle_failure(Buffer, Mod, {Exchange, RoutingKey, Payload0})
    end.

-spec publish_confirmed_async(
    Exchange :: binary(),
    RoutingKey :: binary(),
    PayloadId :: any(),
    Payload :: term()
) -> ok | {error, disconnected} | {error, no_confirms_handler}.
publish_confirmed_async(
    Exchange,
    RoutingKey,
    PayloadId,
    Payload
) ->
    Conn = get(?CONN_KEY),
    publish_confirmed_async_(Conn, Exchange, RoutingKey, PayloadId, Payload).

%% This requires that a confirms handler was added when the connection
%% was started using option `{confirms, Fun/2}`
%% The Id being passed in here will be passed as the Id (argument 2 to the Fun/2)
-spec publish_confirmed_async_(
    amqp_connection(),
    binary(),
    binary(),
    any(),
    any()
) -> ok | {error, disconnected} | {error, no_confirms_handler}.
publish_confirmed_async_(
    #{confirms_handler := undefined}, _Exchange, _RoutingKey, _PayloadId, _Payload
) ->
    {error, no_confirms_handler};
publish_confirmed_async_(
    #{channel := Channel, confirms_handler := ConfirmsHandler},
    Exchange,
    RoutingKey,
    PayloadId,
    Payload0
) ->
    Payload = encode(Payload0),
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Props = #'P_basic'{delivery_mode = 2},
    Msg = #amqp_msg{props = Props, payload = Payload},
    try amqp_channel:next_publish_seqno(Channel) of
        SeqNum ->
            ok = amqp_confirms_handler:add(ConfirmsHandler, SeqNum, PayloadId),
            case amqp_channel:call(Channel, Publish, Msg) of
                ok ->
                    ok;
                Error ->
                    ok = amqp_confirms_handler:delete(ConfirmsHandler, SeqNum),
                    Error
            end
    catch
        E:M ->
            ?LOG_ERROR("SeqNum failure: Reconnect: ~p ~p", [E, M]),
            {error, disconnected}
    end.

wait_for_confirms(#{confirms_handler := ConfirmsHandler}) ->
    amqp_confirms_handler:wait_for_confirms(ConfirmsHandler, infinity).

wait_for_confirms(#{confirms_handler := ConfirmsHandler}, Timeout) ->
    amqp_confirms_handler:wait_for_confirms(ConfirmsHandler, Timeout).

%% @doc Synchronous RPC call to the given queue.
%% These cannot be buffered if the connection is lost.
-spec rpc_request(binary(), any()) ->
    ok | {ok, any()} | {error, disconnected}.
rpc_request(Queue, Payload) ->
    Conn = get(?CONN_KEY),
    rpc_request_(Conn, Queue, Payload).

-spec rpc_request_(amqp_connection(), binary(), any()) ->
    ok | {ok, any()} | {error, disconnected}.
rpc_request_(#{conn := Connection}, Queue, Payload) ->
    try
        Client = amqp_rpc_client:start(Connection, Queue),
        Reply = amqp_rpc_client:call(Client, encode(Payload)),
        amqp_rpc_client:stop(Client),
        decode(Reply)
    catch
        E:M ->
            ?LOG_ERROR("RPC request failed: ~p ~p", [E, M]),
            {error, disconnected}
    end.

%% @doc Send a response to an RPC as a synchronous call to the given queue.
%% These cannot be buffered if the connection is lost.
-spec rpc_response(amqp_connection(), deliver_payload(), any()) ->
    ok | {error, disconnected}.
rpc_response(
    #{channel := Channel},
    {#'basic.deliver'{}, #amqp_msg{
        props = #'P_basic'{
            correlation_id = CorrelationId,
            reply_to = ReplyQ
        }
    }},
    Payload
) ->
    Publish = #'basic.publish'{
        exchange = <<>>,
        routing_key = ReplyQ,
        mandatory = true
    },
    Props = #'P_basic'{correlation_id = CorrelationId},
    Msg = #amqp_msg{props = Props, payload = encode(Payload)},
    try amqp_channel:call(Channel, Publish, Msg) of
        ok ->
            ok;
        Error ->
            ?LOG_ERROR("RPC response failed: ~p", [Error]),
            {error, Error}
    catch
        E:M ->
            ?LOG_ERROR("RPC response failed: ~p ~p", [E, M]),
            {error, disconnected}
    end.

%% @doc Create a queue with the given name.
-spec queue(binary()) -> {ok, binary()} | {error, term()}.
queue(QueueName) ->
    Conn = get(?CONN_KEY),
    queue_(Conn, QueueName, []).

queue(QueueName, Opts) ->
    Conn = get(?CONN_KEY),
    queue_(Conn, QueueName, Opts).

%% @doc Create a queue with the given name and options.
-spec queue_(amqp_connection(), binary(), queue_options()) ->
    {ok, binary()} | {error, term()}.
queue_(#{channel := Channel}, QueueName, Opts) ->
    Arguments = prepare_queue_arguments(Opts),
    Declare = #'queue.declare'{
        queue = QueueName,
        durable = proplists:get_value(durable, Opts, true),
        auto_delete = proplists:get_value(auto_delete, Opts, false),
        exclusive = proplists:get_value(exclusive, Opts, false),
        arguments = Arguments
    },
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Declare),
    {ok, Queue}.

prepare_queue_arguments(Opts) ->
    prepare_queue_arguments(Opts, []).

prepare_queue_arguments([], Args) ->
    Args;
prepare_queue_arguments([{expires, V} | Opts], Args) when is_integer(V) ->
    prepare_queue_arguments(Opts, [{"x-expires", signedint, V} | Args]);
prepare_queue_arguments([{mode, V} | Opts], Args) when is_list(V) ->
    prepare_queue_arguments(Opts, [{"x-queue-mode", longstr, V} | Args]);
prepare_queue_arguments([_ | Opts], Args) ->
    prepare_queue_arguments(Opts, Args).

%% @doc Subscribe to a queue with the given name.
%% This will create a new consumer tag for the given queue.
%% The consumer tag will be returned in the reply.
subscribe(Q) ->
    Conn = get(?CONN_KEY),
    subscribe_(Conn, Q, 1, self()).

subscribe(Q, Prefetch) ->
    Conn = get(?CONN_KEY),
    subscribe_(Conn, Q, Prefetch, self()).

subscribe(Q, Prefetch, ConnPid) ->
    #{conn := Conn} = get(?CONN_KEY),
    subscribe_(Conn, Q, Prefetch, ConnPid).

%% @doc Subscribe to a queue with the given name.
%% This will create a new consumer tag for the given queue.
%% The consumer tag will be returned in the reply.
-spec subscribe_(amqp_connection(), binary(), pos_integer(), pid()) ->
    ok | {error, term()}.
subscribe_(#{channel := Channel, consumer_tags := Tags} = Conn, Q, Prefetch, ConnPid) ->
    #'basic.qos_ok'{} =
        amqp_channel:call(Channel, #'basic.qos'{prefetch_count = Prefetch}),
    #'basic.consume_ok'{consumer_tag = Tag} =
        amqp_channel:subscribe(
            Channel,
            #'basic.consume'{queue = Q},
            ConnPid
        ),
    receive
        #'basic.consume_ok'{consumer_tag = Tag} ->
            NewConn = Conn#{consumer_tags => [{Q, Tag} | Tags]},
            put(?CONN_KEY, NewConn),
            ok
    after 1000 ->
        {error, {failed_to_setup_queue_reader, Q}}
    end.

-spec unsubscribe(binary()) ->
    ok.
unsubscribe(Q) ->
    #{channel := Channel, consumer_tags := Tags} = Conn = get(?CONN_KEY),
    case lists:keytake(Q, 1, Tags) of
        {value, {Q, Tag}, NewConsumerTags} ->
            amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
            receive
                %% XXX: Can the cancel return something else?  If so
                %% we will wait here forever.
                #'basic.cancel_ok'{consumer_tag = Tag} ->
                    ok
            after 5_000 ->
                ?LOG_ERROR("Timeout waiting for cancel_ok for consumer tag ~p", [Tag]),
                ok
            end,
            NewConn = Conn#{consumer_tags => NewConsumerTags},
            put(?CONN_KEY, NewConn),
            ok;
        false ->
            ok
    end.

-spec unsubscribe_all() ->
    ok.
unsubscribe_all() ->
    #{channel := Channel, consumer_tags := Tags} = Conn = get(?CONN_KEY),
    lists:foreach(
        fun({_Q, Tag}) ->
            amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
            receive
                %% XXX: Can the cancel return something else?  If so
                %% we will wait here forever.
                #'basic.cancel_ok'{consumer_tag = Tag} ->
                    ok
            after 5_000 ->
                ?LOG_ERROR("Timeout waiting for cancel_ok for consumer tag ~p", [Tag]),
                ok
            end
        end,
        Tags
    ),
    put(?CONN_KEY, Conn#{consumer_tags => []}),
    ok.

%% @doc Extract the payload from the message.
-spec payload(deliver_payload()) -> binary().
payload({#'basic.deliver'{}, #amqp_msg{payload = PayloadBin}}) ->
    PayloadBin.

%% @doc Extract the delivery tag from the message.
-spec tag(deliver_payload() | basic_cancel() | basic_reject()) -> any().
tag(#'basic.cancel'{consumer_tag = Tag}) ->
    Tag;
tag(#'basic.reject'{delivery_tag = Tag}) ->
    Tag;
tag({#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{}}) ->
    Tag.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec decode(binary()) -> term().
decode(Bin) ->
    binary_to_term(base64:decode(Bin)).

-spec encode(term()) -> binary().
encode(Term) ->
    base64:encode(term_to_binary(Term)).

%% @doc Unbuffer confirms function.
%% This function is called when the connection is lost and the
%% buffered messages are being sent to a new connection.
%% The function is called with the message type (ack or nack) and
%% the message id.
-spec unbuffer_confirms_fun(module(), none | queue | module()) -> fun().
unbuffer_confirms_fun(_Mod, none) ->
    fun(_, _) -> ok end;
unbuffer_confirms_fun(_Mod, queue) ->
    fun(_, _) -> ok end;
unbuffer_confirms_fun(Mod, BuffMod) ->
    fun
        (ack, Id) ->
            BuffMod:buff_delete(Id);
        (nack, Id) ->
            BuffMod:buff_unlock(Id),
            ?LOG_ERROR("nack received for message ~p. Unlocked", [{Mod, Id}])
    end.
