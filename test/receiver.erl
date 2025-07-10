-module(receiver).
-behaviour(gen_amqp).

-include("common.hrl").

%% API
-export([start_link/0]).

%% gen_amqp callbacks
-export([
    handle_basic_deliver/2,
    handle_connect/1,
    handle_disconnect/2,
    handle_error/4,
    handle_rpc/2,
    handle_queue_deleted/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Record for the state

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    AmqpConfig = [{buffer, none} | ?AMQP_CONF],
    gen_amqp:start_link({local, ?MODULE}, ?MODULE, AmqpConfig, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #rstate{}}.

handle_rpc({Tag, {reply, Msg}}, #rstate{} = State) ->
    ?LOG_INFO("RPC received: ~p", [Msg]),
    gen_amqp:ack(Tag),
    {reply, {ok, Msg}, State};
handle_rpc({Tag, Msg}, #rstate{} = State) ->
    ?LOG_INFO("RPC received: ~p", [Msg]),
    gen_amqp:ack(Tag),
    {reply, ok, State}.

handle_basic_deliver({Tag, Body}, #rstate{recv_count = RecvCount0} = State) ->
    RecvCount = RecvCount0 + 1,
    ?LOG_INFO("Received message: ~p ~p~n", [Tag, Body]),
    gen_amqp:ack(Tag),
    case RecvCount rem 10 of
        0 ->
            timer:sleep(250),
            ?LOG_INFO("Received ~p messages~n", [RecvCount]),
            QLen = gen_amqp:queue_length(?AMQP_LOG_QUEUE),
            ?LOG_INFO("Queue length: ~p~n", [QLen]),
            ok;
        _ ->
            ok
    end,
    {noreply, State#rstate{recv_count = RecvCount}}.

%% This is where you should perform the initial setup of the AMQP connection
handle_connect(State) ->
    ?LOG_INFO("Connected to AMQP server", []),
    ok = gen_amqp:direct_exchange(?AMQP_LOG_EXCHANGE),
    {ok, Queue} = gen_amqp:queue(?AMQP_LOG_QUEUE, [{mode, "lazy"}]),
    ok = gen_amqp:bind(Queue, ?AMQP_LOG_EXCHANGE, ?AMQP_LOG_ROUTING_KEY),
    ok = gen_amqp:subscribe(Queue, ?LOGGER_PREFETCH),
    {noreply, State}.

handle_disconnect(Reason, State) ->
    ?LOG_ERROR("Disconnected from AMQP server: ~p", [Reason]),
    {noreply, State}.

handle_queue_deleted(?AMQP_LOG_QUEUE, #rstate{} = State) ->
    ?LOG_INFO("~p Queue deleted, re-creating and re-subscribing", [?AMQP_LOG_QUEUE]),
    {ok, Queue} = gen_amqp:queue(?AMQP_LOG_QUEUE, [{mode, "lazy"}]),
    ok = gen_amqp:bind(Queue, ?AMQP_LOG_EXCHANGE, ?AMQP_LOG_ROUTING_KEY),
    ok = gen_amqp:subscribe(Queue, ?LOGGER_PREFETCH),
    {noreply, State};
handle_queue_deleted(Queue, #rstate{} = State) ->
    ?LOG_INFO("Queue deleted: ~p", [Queue]),
    {noreply, State}.

handle_error(From, Reason, {Tag, Message}, #rstate{} = State) ->
    ?LOG_ERROR("Error: ~p ~p ~p", [From, Reason, {Tag, Message}]),
    gen_amqp:ack(Tag),
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_INFO("~p received unknown info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(Reason, #rstate{}) ->
    ?LOG_INFO("~p terminated with reason: ~p", [?MODULE, Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
