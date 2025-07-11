-module(gen_amqp_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").

%% Suite callbacks
all() -> [basic_send_receive].

init_per_suite(Config) ->
    %% Any suite-wide setup can go here
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    %% Start the buffer process
    {ok, BufferPid} = buffer:start_link(),
    %% Start receiver process
    {ok, ReceiverPid} = receiver:start_link(),
    %% Start sender process
    {ok, SenderPid} = sender:start_link(),
    [{buffer, BufferPid}, {receiver, ReceiverPid}, {sender, SenderPid} | Config].

end_per_testcase(_Case, Config) ->
    BufferPid = proplists:get_value(buffer, Config),
    ReceiverPid = proplists:get_value(receiver, Config),
    SenderPid = proplists:get_value(sender, Config),
    ReceiverPid ! stop,
    SenderPid ! stop,
    BufferPid ! stop,
    ok.

basic_send_receive(_Config) ->
    timer:sleep(1_000),
    TestMsg = <<"hello_ct">>,
    {ok, Received} = sender:send_rpc({reply, TestMsg}),
    ct:log("Received: ~p", [Received]),
    ?assertEqual(TestMsg, Received).
