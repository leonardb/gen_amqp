%%%-------------------------------------------------------------------
%%% @copyright (C) 2022, SiftLogic LLC
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(amqp_confirms_handler).

-behaviour(gen_server).

%% Public API
-export([
    register/3,
    unregister/1,
    add/3,
    delete/2,
    wait_for_confirms/2
]).

%% gen_server apis
-export([start/3]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("kernel/include/logger.hrl").

-record(state, {conn, channel, func, waiters = [], complete_waiters = []}).

%% The process will receive #basic.ack{} or #basic.nack{}
%% with the SequenceNumber as the delivery_tag value
%% The fun argument should be a 2-arity func with signature
%% fun(ack | nack, Item :: any()) -> ok.
-spec register(
    Connection :: pid(),
    Channel :: pid(),
    fun((ack | nack, any()) -> ok) | {Module :: atom(), Function :: atom()}
) -> {ok, pid()} | {error, term()}.
register(Connection, Channel, Fun) when is_pid(Channel) andalso is_function(Fun, 2) ->
    case start(Connection, Channel, Fun) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            gen_server:cast(Pid, {update_fun, Fun}),
            {ok, Pid}
    end;
register(Connection, Channel, {M, F}) when is_pid(Channel) andalso is_atom(M) andalso is_atom(F) ->
    case erlang:function_exported(M, F, 2) of
        false ->
            {error, {bad_function, {M, F}}};
        true ->
            %% The function is exported, so we can start the handler
            case start(Connection, Channel, {M, F}) of
                {ok, Pid} ->
                    {ok, Pid};
                {error, {already_started, Pid}} ->
                    gen_server:cast(Pid, {update_fun, {M, F}}),
                    {ok, Pid}
            end
    end.

unregister(undefined) ->
    ok;
unregister(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, stop).

add(Pid, Id, PayloadId) when is_pid(Pid) ->
    gen_server:call(Pid, {add, Id, PayloadId}, infinity).

delete(Pid, Id) when is_pid(Pid) ->
    gen_server:cast(Pid, {delete, Id}).

wait_for_confirms(undefined, _Timeout) ->
    ok;
wait_for_confirms(Pid, Timeout) when is_pid(Pid) ->
    gen_server:call(Pid, confirms_received, Timeout).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start(Connection, Channel, Fun) when is_pid(Channel) andalso is_function(Fun, 2) ->
    gen_server:start(?MODULE, [Connection, Channel, Fun], []);
start(Connection, Channel, {M, F}) when is_pid(Channel) andalso is_atom(M) andalso is_atom(F) ->
    gen_server:start(?MODULE, [Connection, Channel, {M, F}], []).

init([Connection, Channel, Fun]) ->
    process_flag(trap_exit, true),
    Self = self(),
    link(Connection),
    amqp_channel:register_confirm_handler(Channel, Self),
    ?LOG_INFO("STARTED confirms handler: ~p for channel ~p", [Self, Channel]),
    {ok, #state{conn = Connection, channel = Channel, func = Fun}}.

handle_call({add, Id, PayloadId}, _From, #state{waiters = Waiters} = State) ->
    {reply, ok, State#state{waiters = [{Id, PayloadId} | Waiters]}};
handle_call(confirms_received, _From, #state{waiters = []} = State) ->
    {reply, ok, State};
handle_call(confirms_received, From, #state{complete_waiters = Waiters} = State) ->
    {noreply, State#state{complete_waiters = [From | Waiters]}};
handle_call(
    stop, _From, #state{conn = Conn, channel = Channel, complete_waiters = Waiters} = State
) ->
    maybe_notify(Waiters, []),
    unlink(Conn),
    amqp_channel:unregister_confirm_handler(Channel),
    {stop, normal, ok, State#state{conn = undefined, channel = undefined, complete_waiters = []}};
handle_call(_Request, _From, #state{} = State) ->
    {reply, ok, State}.

handle_cast({delete, Id}, #state{waiters = Waiters} = State) ->
    NWaiters = lists:keydelete(Id, 1, Waiters),
    {noreply, State#state{waiters = NWaiters}};
handle_cast({update_fun, Fun}, #state{channel = Channel} = State) when is_function(Fun, 2) ->
    ?LOG_INFO("Replaced fun in ~p for channel ~p", [self(), Channel]),
    {noreply, State#state{func = Fun}};
handle_cast({update_fun, {M, F} = Fun}, #state{channel = Channel} = State) when
    is_atom(M) andalso is_atom(F)
->
    ?LOG_INFO("Replaced fun in ~p for channel ~p", [self(), Channel]),
    {noreply, State#state{func = Fun}};
handle_cast(_Request, #state{} = State) ->
    {noreply, State}.

handle_info(
    #'basic.ack'{delivery_tag = Id, multiple = false},
    #state{
        waiters = Waiters,
        func = Fun,
        complete_waiters = CompleteWaiters
    } = State
) ->
    case lists:keytake(Id, 1, Waiters) of
        false ->
            %% io:format("remainingA: ~p~n", [length(Waiters)]),
            NCWaiters = maybe_notify(CompleteWaiters, Waiters),
            {noreply, State#state{complete_waiters = NCWaiters}};
        {value, {Id, Item}, Remaining} ->
            apply_callback(Fun, ack, Item),
            %% io:format("remainingB: ~p~n", [length(Remaining)]),
            NCWaiters = maybe_notify(CompleteWaiters, Remaining),
            {noreply, State#state{waiters = Remaining, complete_waiters = NCWaiters}}
    end;
handle_info(
    #'basic.ack'{delivery_tag = Id, multiple = true},
    #state{
        waiters = Waiters,
        func = Fun,
        complete_waiters = CompleteWaiters
    } = State
) ->
    {Remaining, Succeeded} = lists:partition(fun({K, _V}) -> K > Id end, Waiters),
    lists:foreach(
        fun({_Id, Item}) ->
            apply_callback(Fun, ack, Item)
        end,
        Succeeded
    ),
    %% io:format("remainingC: ~p ~p ~p~n", [length(Remaining), Id, Remaining]),
    NCWaiters = maybe_notify(CompleteWaiters, Remaining),
    {noreply, State#state{waiters = Remaining, complete_waiters = NCWaiters}};
handle_info(
    #'basic.nack'{delivery_tag = Id, multiple = false},
    #state{
        waiters = Waiters,
        func = Fun,
        complete_waiters = CompleteWaiters
    } = State
) ->
    case lists:keytake(Id, 1, Waiters) of
        false ->
            %% io:format("remainingD: ~p~n", [length(Waiters)]),
            NCWaiters = maybe_notify(CompleteWaiters, Waiters),
            {noreply, State#state{complete_waiters = NCWaiters}};
        {value, {Id, Item}, Remaining} ->
            apply_callback(Fun, nack, Item),
            %% io:format("remainingE: ~p~n", [length(Remaining)]),
            NCWaiters = maybe_notify(CompleteWaiters, Remaining),
            {noreply, State#state{waiters = Remaining, complete_waiters = NCWaiters}}
    end;
handle_info(
    #'basic.nack'{delivery_tag = Id, multiple = true},
    #state{
        waiters = Waiters,
        func = Fun,
        complete_waiters = CompleteWaiters
    } = State
) ->
    {Remaining, Failed} = lists:partition(fun({K, _V}) -> K > Id end, Waiters),
    lists:foreach(
        fun({_Id, Item}) ->
            apply_callback(Fun, nack, Item)
        end,
        Failed
    ),
    %% io:format("remainingF: ~p~n", [length(Remaining)]),
    NCWaiters = maybe_notify(CompleteWaiters, Remaining),
    {noreply, State#state{waiters = Remaining, complete_waiters = NCWaiters}};
handle_info(
    {'EXIT', _Pid,
        {function_clause, [
            {amqp_network_connection, handle_message, [{'EXIT', _, _}, _], _}, _, _, _, _
        ]}},
    #state{func = Fun, complete_waiters = CompleteWaiters, waiters = Waiters} = State
) ->
    %% The amqp connection got shut down
    %% reply to waiting processes
    lists:foreach(
        fun({_Id, Item}) ->
            apply_callback(Fun, ack, Item)
        end,
        Waiters
    ),
    maybe_notify(CompleteWaiters, []),
    {noreply, State#state{waiters = []}};
handle_info(
    {'EXIT', _, {shutdown, {server_initiated_close, _, _}}},
    #state{func = Fun, complete_waiters = CompleteWaiters, waiters = Waiters} = State
) ->
    %% The amqp connection got shut down
    %% reply to waiting processes
    lists:foreach(
        fun({_Id, Item}) ->
            apply_callback(Fun, ack, Item)
        end,
        Waiters
    ),
    maybe_notify(CompleteWaiters, []),
    {noreply, State#state{waiters = []}};
handle_info(_Info, #state{} = State) ->
    ?LOG_ERROR("Unknown info: ~p ~p", [_Info, State]),
    {noreply, State}.

terminate(_Reason, #state{} = _State) ->
    ok.

code_change(_OldVsn, #state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
maybe_notify([], _Waiters) ->
    [];
maybe_notify(CompleteWaiters, []) ->
    [gen_server:reply(F, ok) || {_Pid, _Ref} = F <- CompleteWaiters],
    [];
maybe_notify(CompleteWaiters, _Waiters) ->
    CompleteWaiters.

apply_callback({M, F}, AckNack, Item) ->
    M:F(AckNack, Item);
apply_callback(Fun, AckNack, Item) ->
    Fun(AckNack, Item).
