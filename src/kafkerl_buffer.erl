-module(kafkerl_buffer).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([buffer/2, build_request/4]).
-export([get_saved_request/1, delete_saved_request/1]).
% Supervisors
-export([start_link/1]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type start_link_response() :: {ok, pid()} | ignore | error().

-record(state, {max_queue_size  = 0 :: non_neg_integer(),
                message_buffer = [] :: [{integer(), basic_message()}],
                saved_requests = [] :: [{correlation_id(),
                                         iodata(),
                                         [basic_message()]}]}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(any()) -> start_link_response().
start_link(Config) ->
  gen_server:start_link({local, ?ETS_BUFFER}, ?MODULE, [Config], []).

% Returns true if the buffer should be flushed
-spec buffer(atom(), basic_message()) -> {ok, boolean()}.
buffer(Broker, Message) ->
  gen_server:call(?ETS_BUFFER, {buffer, Broker, Message}).

-spec build_request(atom(), client_id(), correlation_id(), compression()) ->
  {ok, iodata() | void}.
build_request(Broker, ClientId, CorrelationId, Compression) ->
  case gen_server:call(?ETS_BUFFER, {get_buffer, Broker}) of
    {ok, []} ->
      {ok, void};
    {ok, Messages} ->
      MergedMessages = merge_topics(Messages),
      Request = kafkerl_protocol:build_produce_request(MergedMessages,
                                                       ClientId,
                                                       CorrelationId,
                                                       Compression),
      SavedRequest = {CorrelationId, Request, Messages},
      ok = gen_server:call(?ETS_BUFFER, {save_request, SavedRequest}),
      {ok, Request}
  end.

-spec get_saved_request(correlation_id()) ->
  {ok, iodata(), [basic_message()]} | {error, not_found}.
get_saved_request(CorrelationId) ->
  gen_server:call(?ETS_BUFFER, {get_saved_request, CorrelationId}).

-spec delete_saved_request(correlation_id()) -> {ok, [basic_message()]} |
                                                {error, not_found}.
delete_saved_request(CorrelationId) ->
  gen_server:call(?ETS_BUFFER, {delete_saved_request, CorrelationId}).

% gen_server callbacks
-spec handle_call(any(), any(), state()) ->
  {reply, any(), state()}.
handle_call({buffer, Broker, Message}, _From, State) ->
  handle_buffer(Broker, Message, State);
handle_call({get_buffer, Broker}, _From, State) ->
  handle_get_buffer(Broker, State);
handle_call({save_request, Request}, _From, State) ->
  handle_save_request(Request, State);
handle_call({get_saved_request, CorrelationId}, _From, State) ->
  handle_get_saved_request(CorrelationId, State);
handle_call({delete_saved_request, CorrelationId}, _From, State) ->
  handle_delete_saved_request(CorrelationId, State).

% Boilerplate
-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info(_Msg, State) -> {noreply, State}.
-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) -> {noreply, State}.
-spec terminate(atom(), state()) -> ok.
terminate(_Reason, _State) -> ok.
-spec code_change(string(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%==============================================================================
%% Handlers
%%==============================================================================
init([Config]) ->
  Schema = [{max_queue_size, non_neg_integer, {default, 10}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [MaxQueueSize]} ->
      {ok, #state{max_queue_size = MaxQueueSize}};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("cache config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_buffer(Broker, Message, State = #state{message_buffer = Messages,
                                              max_queue_size = MaxQueueSize}) ->
  FormattedMessage = format_message(Message),
  F = fun(undefined) -> {1, [FormattedMessage]};
         (L)         -> {length(L) + 1, [FormattedMessage | L]} end,
  {NewMessagesCount, NewMessages} = update_proplist(Broker, F, Messages),
  ShouldFlush = NewMessagesCount >= MaxQueueSize,
  {reply, {ok, ShouldFlush}, State#state{message_buffer = NewMessages}}.

handle_get_buffer(Broker, State = #state{message_buffer = Messages}) ->
  case lists:keytake(Broker, 1, Messages) of
    false ->
      {reply, {ok, []}, State};
    {value, {_, Value}, NewMessages} ->
      NewState = State#state{message_buffer = NewMessages},
      {reply, {ok, lists:reverse(Value)}, NewState}
  end.

handle_save_request(Request, State = #state{saved_requests = Requests}) ->
  {reply, ok, State#state{saved_requests = [Request | Requests]}}.

handle_get_saved_request(CorrelationId, State) ->
  Response = case lists:keyfind(CorrelationId, 1, State#state.message_buffer) of
               {CorrelationId, Request, Messages} ->
                 {ok, {Request, Messages}};
               _ ->
                 {error, not_found}
             end,
  {reply, Response, State}.

handle_delete_saved_request(CorrelationId,
                            State = #state{saved_requests = Requests}) ->
  case lists:keytake(CorrelationId, 1, Requests) of
    false ->
      {reply, {error, not_found}, State};
    {value, {CorrelationId, _Bin, Messages}, NewMessageBuffer} ->
      {reply, {ok, Messages}, State#state{message_buffer = NewMessageBuffer}}
  end.

%%==============================================================================
%% Utils
%%==============================================================================
format_message({Topic, Partitions}) when is_list(Partitions) ->
  {Topic, Partitions};
format_message({Topic, Partition}) ->
  {Topic, [Partition]};
format_message({Topic, Partition, Messages}) when is_list(Messages) ->
  {Topic, [{Partition, Messages}]};
format_message({Topic, Partition, Message}) ->
  {Topic, [{Partition, [Message]}]}.

update_proplist(Key, Fun, Proplist) ->
  update_proplist(Key, Fun, Proplist, []).
update_proplist(Key, Fun, [], Acc) ->
  {Count, List} = Fun(undefined),
  {Count, lists:reverse([{Key, List} | Acc])};
update_proplist(Key, Fun, [{Key, Value} | T], Acc) ->
  {Count, List} = Fun(Value),
  {Count, lists:reverse([{Key, List} | Acc], T)};
update_proplist(Key, Fun, [H | T], Acc) ->
  update_proplist(Key, Fun, T, [H | Acc]).

merge_topics(Topics) ->
  merge_topics(Topics, []).
merge_topics([], Acc) ->
  Acc;
merge_topics([H | T], Acc) ->
  merge_topics(T, merge_topic(H, Acc)).

merge_topic(Topic, Topics) ->
  merge_topic(Topic, Topics, []).
merge_topic(Topic, [], Acc) ->
  lists:reverse([Topic | Acc]);
merge_topic({Topic, NewPartitions}, [{Topic, Partitions} | T], Acc) ->
  MergedPartitions = merge_partitions(NewPartitions ++ Partitions),
  lists:reverse(Acc, [{Topic, MergedPartitions} | T]);
merge_topic(Topic, [H | T], Acc) ->
  merge_topic(Topic, T, [H | Acc]).

merge_partitions(Partitions) ->
  merge_partitions(Partitions, []).
merge_partitions([], Acc) ->
  Acc;
merge_partitions([H | T], Acc) ->
  merge_partitions(T, merge_partition(H, Acc)).

merge_partition(Partition, Partitions) ->
  merge_partition(Partition, Partitions, []).
merge_partition(Partition, [], Acc) ->
  lists:reverse([Partition | Acc]);
merge_partition({Partition, NewMessages}, [{Partition, Messages} | T], Acc) ->
  lists:reverse(Acc, [{Partition, NewMessages ++ Messages} | T]);
merge_partition(Topic, [H | T], Acc) ->
  merge_partition(Topic, T, [H | Acc]).