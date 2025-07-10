-module(sender).

-behavior(gen_amqp).

-include("common.hrl").

%% API
-export([start_link/0, send_messages/1, send_rpc/1]).

%% gen_amqp callbacks
-export([handle_basic_deliver/2, handle_connect/1, handle_disconnect/2, handle_error/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% API
send_messages(Count) ->
    spawn(fun() ->
        lists:foreach(
            fun(X) ->
                gen_server:call(?MODULE, {send_message, {X, X}}, infinity)
            end,
            lists:seq(1, Count)
        )
    end).
send_rpc(Msg) ->
    gen_server:call(?MODULE, {send_rpc, Msg}, infinity).

start_link() ->
    AmqpConfig = [{buffer, buffer} | ?AMQP_CONF],
    gen_amqp:start_link({local, ?MODULE}, ?MODULE, AmqpConfig, [], []).

%% gen_server callbacks
init([]) ->
    ?LOG_INFO("init ~p", [?MODULE]),
    process_flag(trap_exit, true),
    {ok, #sstate{}}.

handle_basic_deliver({Tag, Body}, #sstate{} = State) ->
    ?LOG_INFO("Received message: ~p ~p~n", [Tag, Body]),
    gen_amqp:ack(Tag),
    {noreply, State}.

%% This is where you should perform the initial setup of the AMQP connection
handle_connect(State) ->
    ?LOG_INFO("Connected to AMQP server", []),
    ok = gen_amqp:direct_exchange(?AMQP_LOG_EXCHANGE),
    {ok, Queue} = gen_amqp:queue(?AMQP_LOG_QUEUE, [{mode, "lazy"}]),
    ok = gen_amqp:bind(Queue, ?AMQP_LOG_EXCHANGE, ?AMQP_LOG_ROUTING_KEY),
    {noreply, State}.

handle_disconnect(Reason, State) ->
    ?LOG_ERROR("Disconnected from AMQP server: ~p", [Reason]),
    {noreply, State}.

handle_error(From, Reason, {Tag, Message}, #sstate{} = State) ->
    ?LOG_ERROR("Error: ~p ~p ~p", [From, Reason, {Tag, Message}]),
    gen_amqp:ack(Tag),
    {noreply, State}.

handle_call(
    {send_message, Message}, _From, #sstate{} = State
) ->
    case
        gen_amqp:publish_confirmed(
            ?AMQP_LOG_EXCHANGE,
            ?AMQP_LOG_ROUTING_KEY,
            Message
        )
    of
        ok ->
            ?LOG_INFO("Sent message: ~p", [Message]),
            {reply, ok, State};
        {ok, buffered} ->
            ?LOG_INFO("Buffered message: ~p", [Message]),
            {reply, ok, State};
        {error, _} = Error ->
            ?LOG_ERROR("Failed to send message: ~p", [Error]),
            {reply, Error, State}
    end;
handle_call(
    {send_rpc, Message}, _From, #sstate{} = State
) ->
    case
        gen_amqp:rpc_request(
            ?AMQP_LOG_QUEUE,
            Message
        )
    of
        ok ->
            ?LOG_INFO("Sent RPC: ~p", [Message]),
            {reply, ok, State};
        {ok, buffered} ->
            ?LOG_INFO("Buffered message: ~p", [Message]),
            {reply, ok, State};
        {error, _} = Error ->
            ?LOG_ERROR("Failed to send message: ~p", [Error]),
            {reply, Error, State};
        Other ->
            ?LOG_ERROR("Other Response: ~p", [Other]),
            {reply, Other, State}
    end;
handle_call(_Request, _From, State) ->
    ?LOG_INFO("~p received unknown call: ~p", [?MODULE, _Request]),
    {reply, ok, State}.

handle_cast({send_message, Message}, #sstate{} = State) ->
    case
        gen_amqp:publish_confirmed(
            ?AMQP_LOG_EXCHANGE,
            ?AMQP_LOG_ROUTING_KEY,
            Message
        )
    of
        ok ->
            {noreply, State};
        {ok, buffered} ->
            {noreply, State};
        {error, _} = Error ->
            ?LOG_ERROR("Failed to send message: ~p", [Error]),
            {noreply, State}
    end;
handle_cast(_Request, State) ->
    ?LOG_INFO("~p received unknown cast: ~p", [?MODULE, _Request]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_INFO("~p received unknown info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
