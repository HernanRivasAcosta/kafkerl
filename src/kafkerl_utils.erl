-module(kafkerl_utils).
-author('hernanrivasacosta@gmail.com').

-export([send_event/2, send_error/2]).
-export([get_tcp_options/1]).
-export([merge_messages/1, split_messages/1, valid_message/1]).
-export([buffer_name/2]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

%%==============================================================================
%% API
%%==============================================================================
-spec send_error(callback(), any()) -> ok.
send_error(Callback, Reason) ->
  send_event(Callback, {error, Reason}).

-spec send_event(callback(), any()) -> ok | {error, {bad_callback, any()}}.
send_event({M, F}, Data) ->
  spawn(fun() -> M:F(Data) end),
  ok;
send_event({M, F, A}, Data) ->
  spawn(fun() -> apply(M, F, A ++ [Data]) end),
  ok;
send_event(Pid, Data) when is_pid(Pid) ->
  Pid ! Data,
  ok;
send_event(Function, Data) when is_function(Function, 1) ->
  spawn(fun() -> Function(Data) end),
  ok;
send_event(BadCallback, _Data) ->
  {error, {bad_callback, BadCallback}}.

get_tcp_options(Options) -> % TODO: refactor
  lists:ukeymerge(1, lists:sort(proplists:unfold(Options)), ?DEFAULT_TCP_OPTS).

% This is rather costly, and for obvious reasons does not maintain the order of
% the partitions or topics, but it does keep the order of the messages within a
% specific topic-partition pair
-spec merge_messages([basic_message()]) -> merged_message().
merge_messages(Topics) ->
  merge_topics(Topics).

% Not as costly, but still avoid this in a place where performance is critical
-spec split_messages(merged_message()) -> [basic_message()].
split_messages({Topic, {Partition, Messages}}) ->
  {Topic, Partition, Messages};
split_messages({Topic, Partitions}) ->
  [{Topic, Partition, Messages} || {Partition, Messages} <- Partitions];
split_messages(Topics) ->
  lists:flatten([split_messages(Topic) || Topic <- Topics]).

-spec valid_message(any()) -> boolean().
valid_message({Topic, Partition, Messages}) ->
  is_binary(Topic) andalso is_integer(Partition) andalso Partition >= 0 andalso
  (is_binary(Messages) orelse is_list_of_binaries(Messages));
valid_message({Topic, Partition}) ->
  is_binary(Topic) andalso (is_partition(Partition) orelse
                            is_partition_list(Partition));
valid_message(L) when is_list(L) ->
  lists:all(fun valid_message/1, L);
valid_message(_Any) ->
  false.

-spec buffer_name(topic(), partition()) -> atom().
buffer_name(Topic, Partition) ->
  Bin = <<Topic/binary, $., (integer_to_binary(Partition))/binary, "_buffer">>,
  binary_to_atom(Bin, utf8).

%%==============================================================================
%% Utils
%%==============================================================================
%% Merge
merge_topics({Topic, Partition, Message}) ->
  merge_topics([{Topic, Partition, Message}]);
merge_topics([{Topic, Partition, Message}]) ->
  [{Topic, [{Partition, Message}]}];
merge_topics(Topics) ->
  merge_topics(Topics, []).

merge_topics([], Acc) ->
  Acc;
merge_topics([{Topic, Partition, Messages} | T], Acc) ->
  merge_topics([{Topic, [{Partition, Messages}]} | T], Acc);
merge_topics([{Topic, Partitions} | T], Acc) ->
  case lists:keytake(Topic, 1, Acc) of
    false ->
      merge_topics(T, [{Topic, merge_partitions(Partitions)} | Acc]);
    {value, {Topic, OldPartitions}, NewAcc} ->
      NewPartitions = Partitions ++ OldPartitions,
      merge_topics(T, [{Topic, merge_partitions(NewPartitions)} | NewAcc])
  end.

merge_partitions(Partitions) ->
  merge_partitions(Partitions, []).

merge_partitions([], Acc) ->
  Acc;
merge_partitions([{Partition, Messages} | T], Acc) ->
  case lists:keytake(Partition, 1, Acc) of
    false ->
      merge_partitions(T, [{Partition, Messages} | Acc]);
    {value, {Partition, OldMessages}, NewAcc} ->
      NewMessages = merge_messages(OldMessages, Messages),
      merge_partitions(T, [{Partition, NewMessages} | NewAcc])
  end.

merge_messages(A, B) ->
  case {is_list(A), is_list(B)} of
    {true, true}   -> B ++ A;
    {false, true}  -> B ++ [A];
    {true, false}  -> [B | A];
    {false, false} -> [B, A]
  end.

is_list_of_binaries(L) when is_list(L) ->
  length(L) > 0 andalso lists:all(fun is_binary/1, L);
is_list_of_binaries(_Any) ->
  false.

is_partition_list(L) when is_list(L) ->
  length(L) > 0 andalso lists:all(fun is_partition/1, L);
is_partition_list(_Any) ->
  false.

is_partition({Partition, Messages}) ->
  is_integer(Partition) andalso Partition >= 0 andalso
  (is_binary(Messages) orelse is_list_of_binaries(Messages));
is_partition(_Any) ->
  false.
