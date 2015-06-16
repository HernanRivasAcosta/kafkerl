-module(stress_SUITE).
-author('hernanrivasacosta@gmail.com').

-export([all/0]).
-export([parallel_stress_test/1, sequential_stress_test/1]).

-type config() :: [{atom(), term()}].

-spec all() -> [atom()].
all() ->
  [parallel_stress_test, sequential_stress_test].

%%==============================================================================
%% Tests
%%==============================================================================
-spec parallel_stress_test(config()) -> ok.
parallel_stress_test(_Config) ->
  Topics = [<<"stress_test0">>, <<"stress_test1">>, <<"stress_test2">>],
  Partitions = [0, 1, 2],
  MessageCount = 10000,
  
  Self = self(),
  Pids = [spawn(fun() ->
                  Self ! produce(C, T, P, get_prefix(T, P))
                end) ||
          T <- Topics, P <- Partitions, C <- lists:seq(1, MessageCount)],
  ok = receive_all(length(Pids)).

-spec sequential_stress_test(config()) -> ok.
sequential_stress_test(_Config) ->
  Topic = <<"stress_test1">>,
  Partition = 0,
  MessageCount = 10000,
  _ = [produce(C, Topic, Partition, get_prefix(Topic, Partition)) ||
       C <- lists:seq(1, MessageCount)],
  ok.

%%==============================================================================
%% Utils
%%==============================================================================
produce(MessageValue, Topic, Partition, Prefix) ->
  Message = <<Prefix/binary, (integer_to_binary(MessageValue))/binary>>,
  kafkerl:produce({Topic, Partition, Message}).

get_prefix(Topic, Partition) ->
  CommonPrefix = <<"test_message_">>,
  <<CommonPrefix/binary, Topic/binary, $_, (Partition + $0), $_>>.

receive_all(0) ->
  ok;
receive_all(Count) ->
  receive
    _ ->
      receive_all(Count - 1)
  after
    5000 ->
      error
  end.