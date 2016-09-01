-module(kafkerl_test_utils).
-author('hernanrivasacosta@gmail.com').

-export([start_kafka/0, start_kafka/1, create_test_topics/0, stop_kafka/0]).

%%==============================================================================
%% API
%%==============================================================================
-spec start_kafka() -> ok.
start_kafka() ->
  start_kafka(false).

-spec start_kafka(boolean()) -> ok.
start_kafka(CreateTestTopics) ->
  % Clean all the logs
  lager:critical("1"),
  [] = os:cmd("rm -rf bin/tmp"),
  % Start zookeeper and kafka
  lager:critical("2"),
  Path = get_path(),
  lager:critical("3"),
  [] = os:cmd("./bin/start_zk.sh -d " ++ Path ++ " -c bin/zookeeper.properties"),
  lager:critical("4"),
  [] = os:cmd("./bin/start_broker.sh -d " ++ Path ++ " -c bin/server0.properties"),
  lager:critical("5"),
  [] = os:cmd("./bin/start_broker.sh -d " ++ Path ++ " -c bin/server1.properties"),
  lager:critical("6"),
  [] = os:cmd("./bin/start_broker.sh -d " ++ Path ++ " -c bin/server2.properties"),
  lager:critical("7"),
  % Create the test topics and partitions
  case CreateTestTopics of
    true  -> create_test_topics();
    false -> ok
  end.

-spec create_test_topics() -> ok.
create_test_topics() ->
  Path = get_path(),
  % TODO: If kafka doesn't start properly, this will never return
  [] = os:cmd("./bin/create_test_topics.sh -d " ++ Path),
  ok.

-spec stop_kafka() -> ok.
stop_kafka() ->
  % Stop both zookeeper and kafka
  [] = os:cmd("./bin/stop_zk.sh"),
  [] = os:cmd("./bin/stop_all_brokers.sh"),
  ok.

%%==============================================================================
%% Utils
%%==============================================================================
get_path() ->
  {ok, TestProps} = application:get_env(kafkerl, tests),
  proplists:get_value(kafkerl_path, TestProps).