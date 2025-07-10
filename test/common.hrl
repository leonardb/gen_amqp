-include_lib("kernel/include/logger.hrl").

-define(AMQP_CONF, [
    {host, {127, 0, 0, 1}},
    {virtual_host, ?LOGGING_VHOST},
    {username, <<"funny">>},
    {password, <<"bunny">>},
    {heartbeat, 10},
    {confirms, fun
        (ack, Ids) when is_list(Ids) ->
            [?LOG_INFO("Done id: ~p~n", [Id]) || Id <- Ids],
            ok;
        (ack, Id) ->
            ?LOG_INFO("Done id: ~p~n", [Id]),
            ok;
        (nack, _Id) ->
            ok;
        (Other, Id) ->
            ?LOG_INFO("Other: ~p Id: ~p~n", [Other, Id]),
            ok
    end}
]).
-define(AMQP_LOG_EXCHANGE, <<"logging_v2">>).
-define(LOGGING_VHOST, <<"/trklogging">>).
-define(AMQP_LOG_QUEUE, <<"logging_v3">>).
-define(AMQP_LOG_ROUTING_KEY, <<"logging_v2">>).
-define(LOGGER_PREFETCH, 50).

-record(rstate, {recv_count = 0}).
-record(sstate, {}).
