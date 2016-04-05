-module(kafkerl_utils).
-author('hernanrivasacosta@gmail.com').

-export([send_event/2, send_error/2]).
-export([get_tcp_options/1]).
-export([merge_messages/1, split_messages/1, valid_message/1]).
-export([buffer_name/2, default_buffer_name/0]).
-export([gather_consume_responses/0, gather_consume_responses/1]).
-export([proplists_set/2]).

%%==============================================================================
%% API
%%==============================================================================
-spec send_error(kafkerl:callback(), any()) -> ok.
send_error(Callback, Reason) ->
  send_event(Callback, {error, Reason}).

-spec send_event(kafkerl:callback(), any()) -> ok | {error, {bad_callback, any()}}.
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

default_tcp_options() ->
  % This list has to be sorted
  [{mode, binary}, {packet, 0}].
get_tcp_options(Options) -> % TODO: refactor
  UnfoldedOptions = proplists:unfold(Options),
  lists:ukeymerge(1, lists:sort(UnfoldedOptions), default_tcp_options()).

% This is rather costly, and for obvious reasons does not maintain the order of
% the partitions or topics, but it does keep the order of the messages within a
% specific topic-partition pair
-spec merge_messages([kafkerl_protocol:basic_message()]) ->
  kafkerl_protocol:merged_message().
merge_messages(Topics) ->
  merge_topics(Topics).

% Not as costly, but still avoid this in a place where performance is critical
-spec split_messages(kafkerl_protocol:merged_message()) ->
  [kafkerl_protocol:basic_message()].
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

-spec buffer_name(kafkerl_protocol:topic(), kafkerl_protocol:partition()) ->
  atom().
buffer_name(Topic, Partition) ->
  Bin = <<Topic/binary, $., (integer_to_binary(Partition))/binary, "_buffer">>,
  binary_to_atom(Bin, utf8).

-spec default_buffer_name() -> atom().
default_buffer_name() ->
  default_message_buffer.

-type proplist_value() :: {atom(), any()}.
-type proplist()       :: [proplist_value].
-spec proplists_set(proplist(), proplist_value() | [proplist_value()]) ->
  proplist().
proplists_set(Proplist, {K, _V} = NewValue) ->
  lists:keyreplace(K, 1, Proplist, NewValue);
proplists_set(Proplist, []) ->
  Proplist;
proplists_set(Proplist, [H | T]) ->
  proplists_set(proplists_set(Proplist, H), T).

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

gather_consume_responses() ->
  gather_consume_responses(2500).
gather_consume_responses(Timeout) ->
  gather_consume_responses(Timeout, []).
gather_consume_responses(Timeout, Acc) ->
  receive
    {consumed, Messages} ->
      gather_consume_responses(Timeout, Acc ++ Messages);
    {offset, Offset} ->
      {Acc, Offset};
    {error, _Reason} = Error ->
      Error
  after Timeout ->
    []
  end.