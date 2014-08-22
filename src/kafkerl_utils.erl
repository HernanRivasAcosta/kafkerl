-module(kafkerl_utils).
-author('hernanrivasacosta@gmail.com').

-export([send_event/2, send_error/2]).
-export([get_tcp_options/1]).
-export([merge_messages/1]).

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

%%==============================================================================
%% Utils
%%==============================================================================
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