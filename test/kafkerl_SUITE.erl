-module(kafkerl_SUITE).
-author('hernanrivasacosta@gmail.com').

-export([produce_and_consume/1]).

-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2, all/0]).

-type config() :: [{atom(), term()}].

-spec all() -> [atom()].
all() ->
  [produce_and_consume].

-spec init_per_suite(config()) -> [config()].
init_per_suite(Config) ->
  Config.

-spec end_per_suite(config()) -> [config()].
end_per_suite(Config) ->
  Config.

-spec init_per_testcase(atom(), config()) -> [config()].
init_per_testcase(TestCase, Config) ->
  Config.

-spec end_per_testcase(atom(), config()) -> [config()].
end_per_testcase(TestCase, Config) ->
  kafkerl_test_utils:stop_kafka(),
  Config.

%%==============================================================================
%% Tests
%%==============================================================================
-spec produce_and_consume(config()) -> ok.
produce_and_consume(_Config) ->
  % Start by producing a message while kafkerl has not been started
  ct:pal("sending initial message"),
  {error, not_started} = kafkerl:produce(<<"kafkerl_test3">>, 0, <<"ignore">>),
  % Start kafkerl
  ct:pal("starting kafkerl"),
  ok = kafkerl:start(),
  % Produce on some non existing topic, it will be cached
  ct:pal("producing a message that will be cached"),
  {ok, cached} = kafkerl:produce(<<"kafkerl_test3">>, 0, <<"msg1">>),
  % Start kafka
  ct:pal("starting kafkerl"),
  ok = kafkerl_test_utils:start_kafka(),
  % Create the topics and get the metadata
  %ct:pal("create the topics"),
  %ok = kafkerl_test_utils:create_test_topics(),
  ct:pal("request the metadata"),
  ok = kafkerl:request_metadata(),
  % Wait for the metadata to be updated
  ok = receive
         {partition_update, PU = [_ | _]} ->
           ct:pal("got an update (~p)!", [PU]),
           ok
       after 7500 ->
         ct:pal("no update :("),
         error
       end,
  % Send another message
  ct:pal("send a message"),
  {ok, saved} = kafkerl:produce(<<"kafkerl_test3">>, 0, <<"msg2">>),
  % Wait a bit for the messages to be sent

  ok.