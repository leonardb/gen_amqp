%% @doc Buffer module provides a simple caching mechanism
%% with locking capabilities.
%% This module implements a gen_server that maintains an
%% ETS table for temporary data storage with automatic
%% unlocking of stale locks.
%%
%% Implementation details:
%% For amqp buffering there are 4x required functions:
%% - buff_lock(Mod, Limit) -> {ok, [{Id, Data}]} | {ok, []}
%% - buff_unlock(Id) -> ok | {error, not_found}
%% - buff_put(Mod, Data) -> ok
%% - buff_delete(Id) -> ok
-module(buffer).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% Buffer API
-export([
    buff_put/2,
    buff_lock/2,
    buff_unlock/1,
    buff_delete/1
]).

%% gen_server API
-export([start_link/0, stop/0]).

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(cache, {
    id :: integer() | last_id | '_',
    mod :: atom() | integer(),
    data :: any(),
    locked = undefined :: undefined | calendar:datetime()
}).

%% @doc Locks and retrieves up to Limit records for a specific module.
%% This is called when the amqp connection comes back online
-spec buff_lock(Mod :: module(), Count :: non_neg_integer()) -> {ok, [term()]}.
buff_lock(Mod, Limit) ->
    gen_server:call(?SERVER, {lock, Mod, Limit}).

%% @doc Unlocks a previously locked record (when we receive a nack)
-spec buff_unlock(Id :: pos_integer()) -> ok.
buff_unlock(Id) ->
    gen_server:call(?SERVER, {unlock, Id}).

%% @doc Inserts a new record into the buffer
-spec buff_put(Mod :: module(), Msg :: term()) -> ok.
buff_put(Mod, Data) ->
    gen_server:call(?SERVER, {put, Mod, Data}).

%% @doc Deletes a record from the buffer when the confirmed ack is received
-spec buff_delete(Id :: pos_integer()) -> ok.
buff_delete(Id) ->
    gen_server:call(?SERVER, {delete, Id}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).

init([]) ->
    ?SERVER = ets:new(?SERVER, [{keypos, 2}, private, named_table, ordered_set]),
    try ets:update_counter(?SERVER, last_id, {3,0}) of
        _LastId ->
            ok
    catch
        error:badarg ->
            ets:insert_new(?SERVER, #cache{id = last_id, mod = 0})
    end,
    {ok, #{}, timer:seconds(60)}.

handle_call({lock, Mod, Limit}, _From, State) ->
    %% Select up to Limit unlocked records,
    %% mark them as locked and return their {Id, data} pairs
    Ms = [
        {
            #cache{
                id = '_',
                mod = Mod,
                data = '_',
                locked = undefined
            },
            [],
            ['$_']
        }
    ],
    case ets:select(?SERVER, Ms, Limit) of
        '$end_of_table' ->
            {reply, {ok, []}, State};
        {Rows0, _Cont} ->
            Now = erlang:universaltime(),
            Rows = [
                begin
                    ets:insert(?SERVER, Row#cache{locked = Now}),
                    {Id, Data}
                end
             || #cache{id = Id, data = Data} = Row <- Rows0
            ],
            {reply, {ok, Rows}, State}
    end;
handle_call({unlock, Id}, _From, State) ->
    %% Unlock record with Id
    case ets:lookup(?SERVER, Id) of
        [] ->
            {reply, {error, not_found}, State};
        [#cache{} = R] ->
            ets:insert(?SERVER, R#cache{locked = undefined}),
            {reply, ok, State}
    end;
handle_call({put, Mod, Data}, _From, State) ->
    %% Insert new record
    true = ets:insert_new(?SERVER, #cache{
        id = ets:update_counter(?SERVER, last_id, {3, 1}),
        mod = Mod,
        data = Data
    }),
    {reply, ok, State};
handle_call({delete, Id}, _From, State) ->
    %% Delete record with Id
    ets:delete(?SERVER, Id),
    {reply, ok, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    %% unlock records that have been locked for longer than 5 minutes
    %% get timestamp for 5 minutes ago as a calendar:datetime()
    NowSec = calendar:datetime_to_gregorian_seconds(erlang:universaltime()),
    Limit = calendar:gregorian_seconds_to_datetime(NowSec - 300),
    Ms = ets:fun2ms(fun(#cache{locked = Locktime} = R) when Locktime < Limit -> R end),
    case ets:select(?SERVER, Ms, 100) of
        '$end_of_table' ->
            {noreply, State, timer:seconds(60)};
        {Rows, _Cont} ->
            [ets:insert(?SERVER, Row#cache{locked = undefined}) || Row <- Rows],
            {noreply, State, 500}
    end;
handle_info(stop, State) ->
    ?LOG_INFO("Stopping buffer"),
    {stop, normal, ok, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
