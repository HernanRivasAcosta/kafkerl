-module(message_validation_SUITE).
-author('hernanrivasacosta@gmail.com').

-export([valid_messages/1, invalid_messages/1]).

-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2, all/0]).

-type config() :: [{atom(), term()}].

-spec all() -> [atom()].
all() ->
  [valid_messages, invalid_messages].

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
  Config.

%%==============================================================================
%% Tests
%%==============================================================================
-spec valid_messages(config()) -> ok.
valid_messages(_Config) ->
  true = kafkerl_producer:valid_topics({<<"topic">>, 1, <<"msg">>}),
  true = kafkerl_producer:valid_topics({<<"topic">>, {1, <<"msg">>}}),
  true = kafkerl_producer:valid_topics({<<"topic">>, [{1, <<"msg">>}]}),
  true = kafkerl_producer:valid_topics({<<"topic">>, [{1, <<"msg1">>},
                                                      {2, <<"msg2">>}]}),
  true = kafkerl_producer:valid_topics([{<<"topic">>, 1, <<"msg">>}]),
  true = kafkerl_producer:valid_topics([{<<"topic1">>, 1, <<"msg1">>},
                                        {<<"topic2">>, 1, <<"msg2">>}]),
  true = kafkerl_producer:valid_topics([{<<"topic1">>, {1, <<"msg1">>}},
                                        {<<"topic2">>, 1, <<"msg2">>}]),
  true = kafkerl_producer:valid_topics([{<<"topic1">>, [{1, <<"msg1">>}]},
                                        {<<"topic2">>, {1, <<"msg2">>}}]),
  true = kafkerl_producer:valid_topics([{<<"topic1">>, [{1, <<"msg1">>},
                                                        {2, <<"msg2">>}]},
                                        {<<"topic2">>, {1, <<"msg3">>}}]),
  ok.

-spec invalid_messages(config()) -> ok.
invalid_messages(_Config) ->
  false = kafkerl_producer:valid_topics(<<"test">>),
  false = kafkerl_producer:valid_topics({<<"test">>, 1}),
  false = kafkerl_producer:valid_topics({<<"test">>, <<"msg">>}),
  false = kafkerl_producer:valid_topics({<<"test">>, [<<"msg">>]}),
  false = kafkerl_producer:valid_topics({<<"test">>, [1, <<"msg">>]}),
  false = kafkerl_producer:valid_topics([]),
  false = kafkerl_producer:valid_topics([<<"test">>]),
  false = kafkerl_producer:valid_topics({undefined, 1, <<"msg">>}),
  false = kafkerl_producer:valid_topics({<<"topic">>, 1, undefined}),
  false = kafkerl_producer:valid_topics([{<<"topic1">>, [{1, <<"msg1">>},
                                                         {2, undefined}]},
                                         {<<"topic2">>, {1, <<"msg3">>}}]),
  ok.