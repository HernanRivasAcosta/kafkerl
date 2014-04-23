-module(kafkerl_consumer).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

-export([request_topics/2, request_topics/3]).

-export([start_link/1, start_link/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type callback() :: {atom(), atom()}.
-record(state, {connector_name = undefined :: atom() | undefined,
                client_id      = undefined :: binary(),
                max_wait               = 0 :: integer(),
                min_bytes              = 1 :: integer(),
                correlation_id         = 0 :: integer(),
                callbacks             = [] :: [{{kafkerl_topic(),
                                                 kafkerl_partition()},
                                                callback()}]}).

-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-type start_link_response() :: {ok, pid()} | ignore | {error, any()}.

% Starting the server
-spec start_link(any()) -> start_link_response().
start_link(Options) ->
  start_link(?MODULE, Options).
-spec start_link(atom(), any()) -> start_link_response().
start_link(Name, Options) when is_atom(Name) ->
  gen_server:start_link({local, Name}, ?MODULE, [Options], []).

% Requesting topics
-spec request_topics(kafkerl_fetch_request(), callback()) -> ok.
request_topics(Topics, Callback) ->
  request_topics(?MODULE, Topics, Callback).
-spec request_topics(atom(), kafkerl_fetch_request(), callback()) -> ok.
request_topics(Name, Topics, Callback) ->
  gen_server:cast(Name, {request_topics, Topics, Callback}).

% gen_server callbacks
-type valid_cast_message() :: {request_topics, kafkerl_fetch_request(),
                               callback()}.

-spec handle_cast(valid_cast_message(), state()) -> {noreply, state()}.
handle_cast({request_topics, Topic, Callback}, State) when is_tuple(Topic) ->
  handle_cast({request_topics, [Topic], Callback}, State);  
handle_cast({request_topics, Topics, Callback},
            State = #state{callbacks = OldCallbacks,
                           correlation_id = CorrelationId}) ->
  FlatTopics = lists:flatten(Topics),
  case handle_request_topics(FlatTopics, State) of
    ok ->
      TopicandPartitions = get_topic_partition_tuples(FlatTopics),
      NewCallbacks = [{Tuple, Callback} || Tuple <- TopicandPartitions],
      NewState = State#state{correlation_id = CorrelationId + 1,
                             callbacks = NewCallbacks ++ OldCallbacks},
      {noreply, NewState};
    Error ->
      lager:error("an error occurred when requesting topics ~p", [Error]),
      {noreply, State}
  end.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({kafka_message, Bin}, State = #state{callbacks = Callbacks}) ->
  lager:info("reveived bin: ~p", [Bin]),
  ok = on_binary_received(Bin, Callbacks),
  {noreply, State};
handle_info(Msg, State) ->
  lager:notice("Unexpected info message received: ~p on ~p", [Msg, State]),
  {noreply, State}.

% Boilerplate
-spec handle_call(any(), any(), state()) -> {reply, ok, state()}.
handle_call(_Msg, _From, State) -> {reply, ok, State}.
-spec terminate(atom(), state()) -> ok.
terminate(_Reason, _State) -> ok.
-spec code_change(string(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%==============================================================================
%% Handlers
%%==============================================================================
init([Options]) ->
  Schema = [{connector, atom, required},
            {client_id, binary, required},
            {max_wait, integer, {default, 1000}},
            {min_bytes, integer, {default, 10}},
            {correlation_id, integer, {default, 0}}],
  case normalizerl:normalize_proplist(Schema, Options) of
    {ok, [ConnectorName, ClientId, MaxWait, MinBytes, CorrelationId]} ->
      kafkerl_connector:add_tcp_listener(ConnectorName, self()),
      {ok, #state{connector_name = ConnectorName, client_id = ClientId,
                  correlation_id = CorrelationId, max_wait = MaxWait,
                  min_bytes = MinBytes}};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("Consumer config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_request_topics(Topics, #state{connector_name = ConnectorName,
                                     client_id = ClientId,
                                     correlation_id = CorrelationId,
                                     max_wait = MaxWait,
                                     min_bytes = MinBytes}) ->
  Req = kafkerl_protocol:build_fetch_request(Topics, ClientId, CorrelationId,
                                             MaxWait, MinBytes),
  kafkerl_connector:send(ConnectorName, Req).

on_binary_received(Binary, Callbacks) ->
  ok = case kafkerl_protocol:parse_fetch_response(Binary) of
         {ok, _} ->
           ok;
         {error, Reason} ->
           lager:error("unable to read kafka message, reason: ~p", [Reason])
       end.

%%==============================================================================
%% Utils
%%==============================================================================
get_topic_partition_tuples(Topics) ->
  get_topic_partition_tuples(Topics, []).

get_topic_partition_tuples([], R) ->
  R;
get_topic_partition_tuples([{Topic, {Partition, _, _}} | Topics], R) ->
  get_topic_partition_tuples(Topics, [{Topic, Partition} | R]);
get_topic_partition_tuples([{_Topic, []} | Topics], R) ->
  get_topic_partition_tuples(Topics, R);
get_topic_partition_tuples([{Topic, [Partition | Partitions]} | Topics], R) ->
  get_topic_partition_tuples([{Topic, Partitions} | Topics],
                             [{Topic, Partition} | R]).