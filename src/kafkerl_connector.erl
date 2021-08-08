-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

%% API
% Metadata
-export([request_metadata/1, request_metadata/2, request_metadata/3,
         get_partitions/1]).
% Produce
-export([send/3]).
% Consume
-export([fetch/4, stop_fetch/3]).
% Common
-export([subscribe/2, subscribe/3, unsubscribe/2]).
% Only for internal use
-export([do_request_metadata/6, make_metadata_request/1]).
% Only for broker connections
-export([produce_succeeded/2]).
% Supervisors
-export([start_link/2]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type kafler_host() :: string().
-type kafler_port() :: 1..65535.
-type address()     :: {kafler_host(), kafler_port()}.

-type filters() :: all | [atom()].

-type broker_mapping_key()  :: {kafkerl:topic(), kafkerl:partition()}.
-type broker_mapping()      :: {broker_mapping_key(), kafkerl:server_ref()}.

-record(state, {brokers               = [] :: [address()],
                broker_mapping      = void :: [broker_mapping()] | void,
                client_id           = <<>> :: kafkerl_protocol:client_id(),
                max_metadata_retries  = -1 :: integer(),
                retry_interval         = 1 :: non_neg_integer(),
                config                = [] :: {atom(), any()},
                autocreate_topics  = false :: boolean(),
                callbacks             = [] :: [{filters(), kafkerl:callback()}],
                known_topics          = [] :: [binary()],
                pending               = [] :: [kafkerl:basic_message()],
                last_metadata_request  = 0 :: integer(),
                metadata_request_cd    = 0 :: integer(),
                last_dump_name   = {"", 0} :: {string(), integer()},
                default_fetch_options = [] :: kafkerl:options()}).
-type state() :: #state{}.

-export_type([address/0]).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(atom(), any()) -> {ok, pid()} | ignore | kafkerl:error().
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config], []).

-spec send(kafkerl:server_ref(), kafkerl:basic_message(), kafkerl:options()) ->
  ok | kafkerl:error().
send(ServerRef, {Topic, Partition, _Payload} = Message, Options) ->
  Buffer = kafkerl_utils:buffer_name(Topic, Partition),
  case ets_buffer:write(Buffer, Message) of
    NewSize when is_integer(NewSize) ->
      case lists:keyfind(buffer_size, 1, Options) of
        {buffer_size, MaxSize} when NewSize > MaxSize ->
          gen_server:call(ServerRef, {dump_buffer_to_disk, Buffer, Options});
        _ ->
          ok
      end;
    Error ->
      ok = ?LOG_DEBUG("unable to write on ~p, reason: ~p", [Buffer, Error]),
      gen_server:call(ServerRef, {send, Message})
  end.

-spec fetch(kafkerl:server_ref(), kafkerl:topic(), kafkerl:partition(),
            kafkerl:options()) -> ok | kafkerl:error().
fetch(ServerRef, Topic, Partition, Options) ->
  gen_server:call(ServerRef, {fetch, Topic, Partition, Options}).

-spec stop_fetch(kafkerl:server_ref(), kafkerl:topic(), kafkerl:partition()) ->
  ok.
stop_fetch(ServerRef, Topic, Partition) ->
  gen_server:call(ServerRef, {stop_fetch, Topic, Partition}).

-spec get_partitions(kafkerl:server_ref()) ->
  [{kafkerl:topic(), [kafkerl:partition()]}] | kafkerl:error().
get_partitions(ServerRef) ->
  case gen_server:call(ServerRef, {get_partitions}) of
    {ok, Mapping} ->
      get_partitions_from_mapping(Mapping);
    Error ->
      Error
  end.

-spec subscribe(kafkerl:server_ref(), kafkerl:callback()) ->
  ok | kafkerl:error().
subscribe(ServerRef, Callback) ->
  subscribe(ServerRef, Callback, all).
-spec subscribe(kafkerl:server_ref(), kafkerl:callback(), filters()) ->
  ok | kafkerl:error().
subscribe(ServerRef, Callback, Filter) ->
  gen_server:call(ServerRef, {subscribe, {Filter, Callback}}).
-spec unsubscribe(kafkerl:server_ref(), kafkerl:callback()) -> ok.
unsubscribe(ServerRef, Callback) ->
  gen_server:call(ServerRef, {unsubscribe, Callback}).

-spec request_metadata(kafkerl:server_ref()) -> ok.
request_metadata(ServerRef) ->
  gen_server:call(ServerRef, {request_metadata}).

-spec request_metadata(kafkerl:server_ref(), [kafkerl:topic()] | boolean()) ->
  ok.
request_metadata(ServerRef, TopicsOrForced) ->
  gen_server:call(ServerRef, {request_metadata, TopicsOrForced}).

-spec request_metadata(kafkerl:server_ref(), [kafkerl:topic()], boolean()) ->
  ok.
request_metadata(ServerRef, Topics, Forced) ->
  gen_server:call(ServerRef, {request_metadata, Topics, Forced}).

-spec produce_succeeded(kafkerl:server_ref(), {kafkerl:topic(),
                                               kafkerl:partition(),
                                               [binary()],
                                               integer()}) -> ok.
produce_succeeded(ServerRef, Messages) ->
  gen_server:cast(ServerRef, {produce_succeeded, Messages}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()} |
                                            {reply, {error, any()}, state()}.
handle_call({dump_buffer_to_disk, Buffer, Options}, _From, State) ->
  {DumpNameStr, _} = DumpName = get_ets_dump_name(State#state.last_dump_name),
  AllMessages = ets_buffer:read_all(Buffer),
  FilePath = proplists:get_value(dump_location, Options, "") ++ DumpNameStr,
  _ = case file:write_file(FilePath, term_to_binary(AllMessages)) of
        ok    -> ?LOG_DEBUG("Dumped unsent messages at ~p", [FilePath]);
        Error -> ?LOG_CRITICAL("Unable to save messages, reason: ~p", [Error])
      end,
  {reply, ok, State#state{last_dump_name = DumpName}};
handle_call({send, Message}, _From, State) ->
  handle_send(Message, State);
handle_call({fetch, Topic, Partition, Options}, _From, State) ->
  {reply, handle_fetch(Topic, Partition, Options, State), State};
handle_call({stop_fetch, Topic, Partition}, _From, State) ->
  {reply, handle_stop_fetch(Topic, Partition, State), State};
handle_call({request_metadata}, _From, State) ->
  {reply, ok, handle_request_metadata(State, [])};
handle_call({request_metadata, Forced}, _From, State) when is_boolean(Forced) ->
  {reply, ok, handle_request_metadata(State, [], true)};
handle_call({request_metadata, Topics}, _From, State) ->
  {reply, ok, handle_request_metadata(State, Topics)};
handle_call({request_metadata, Topics, Forced}, _From, State) ->
  {reply, ok, handle_request_metadata(State, Topics, Forced)};
handle_call({get_partitions}, _From, State) ->
  {reply, handle_get_partitions(State), State};
handle_call({subscribe, Callback}, _From, State) ->
  case send_mapping_to(Callback, State) of
    ok ->
      {reply, ok, State#state{callbacks = [Callback | State#state.callbacks]}};
    _  ->
      {reply, {error, invalid_callback}, State}
  end;
handle_call({unsubscribe, Callback}, _From, State) ->
  NewCallbacks = lists:keydelete(Callback, 2, State#state.callbacks),
  {reply, ok, State#state{callbacks = NewCallbacks}}.

-spec handle_info(atom() | {atom(), [] | map()}, state()) ->
  {stop, {error, unable_to_retrieve_metadata}, state()} |
  {noreply, state()}.
handle_info(metadata_timeout, State) ->
  {stop, {error, unable_to_retrieve_metadata}, State};
handle_info({metadata_updated, []}, State) ->
  % If the metadata arrived empty request it again
  {noreply, handle_request_metadata(State#state{broker_mapping = []}, [])};
handle_info({metadata_updated, Mapping}, State) ->
  % Create the topic mapping (this also starts the broker connections)
  NewBrokerMapping = get_broker_mapping(Mapping, State),
  ok = ?LOG_DEBUG("Refreshed topic mapping: ~p", [NewBrokerMapping]),
  % Get the partition data to send to the subscribers and send it
  PartitionData = get_partitions_from_mapping(NewBrokerMapping),
  Callbacks = State#state.callbacks,
  NewCallbacks = send_event({partition_update, PartitionData}, Callbacks),
  % Add to the list of known topics
  NewTopics = lists:sort([T || {T, _P} <- PartitionData]),
  NewKnownTopics = lists:umerge(NewTopics, State#state.known_topics),
  ok = ?LOG_DEBUG("Known topics: ~p", [NewKnownTopics]),
  % Reverse the pending messages and try to send them again
  RPending = lists:reverse(State#state.pending),
  ok = lists:foreach(fun(P) -> send(self(), P, []) end, RPending),
  {noreply, State#state{broker_mapping = NewBrokerMapping, pending = [],
                        callbacks = NewCallbacks,
                        known_topics = NewKnownTopics}};
handle_info({'DOWN', Ref, process, _, normal}, State) ->
  true = demonitor(Ref),
  {noreply, State};
handle_info({'DOWN', Ref, process, _, Reason}, State) ->
  ok = ?LOG_ERROR("metadata request failed, reason: ~p", [Reason]),
  true = demonitor(Ref),
  {noreply, handle_request_metadata(State, [], true)};
handle_info(Msg, State) ->
  ok = ?LOG_NOTICE("Unexpected info message received: ~p on ~p", [Msg, State]),
  {noreply, State}.

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast({produce_succeeded, Messages}, State) ->
  Callbacks = State#state.callbacks,
  NewCallbacks = send_event({produced, Messages}, Callbacks),
  {noreply, State#state{callbacks = NewCallbacks}}.

% Boilerplate
-spec terminate(atom(), state()) -> ok.
terminate(_Reason, _State) -> ok.
-spec code_change(string(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%==============================================================================
%% Handlers
%%==============================================================================
-spec init([{any(), any()}]) -> {ok, state()} | {stop, bad_config}.
init([Config]) ->
  Schema = [{brokers, [{string, {integer, {1, 65535}}}], required},
            {max_metadata_retries, {integer, {-1, undefined}}, {default, -1}},
            {client_id, binary, {default, <<"kafkerl_client">>}},
            {topics, [binary], required},
            {metadata_tcp_timeout, positive_integer, {default, 1500}},
            {assume_autocreate_topics, boolean, {default, false}},
            {metadata_request_cooldown, positive_integer, {default, 333}},
            {consumer_min_bytes, positive_integer, {default, 1}},
            {consumer_max_wait, positive_integer, {default, 1500}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [Brokers, MaxMetadataRetries, ClientId, Topics, RetryInterval,
          AutocreateTopics, MetadataRequestCooldown, MinBytes, MaxWait]} ->
      DefaultFetchOptions = [{min_bytes, MinBytes}, {max_wait, MaxWait}],
      State = #state{config                = Config,
                     known_topics          = Topics,
                     brokers               = Brokers,
                     client_id             = ClientId,
                     retry_interval        = RetryInterval,
                     autocreate_topics     = AutocreateTopics,
                     max_metadata_retries  = MaxMetadataRetries,
                     metadata_request_cd   = MetadataRequestCooldown,
                     default_fetch_options = DefaultFetchOptions},
      {_Pid, _Ref} = make_metadata_request(State),
      {ok, State};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      ok = ?LOG_CRITICAL("Connector config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_send(Message, State = #state{autocreate_topics = false}) ->
  % The topic didn't exist, ignore
  {Topic, _Partition, Payload} = Message,
  ok = ?LOG_ERROR("Dropped ~p sent to non existing topic ~p", [Payload, Topic]),
  {reply, {error, non_existing_topic}, State};
handle_send(Message, State = #state{broker_mapping = void,
                                    pending = Pending}) ->
  % We should consider saving this to a new buffer instead of using the state.
  {reply, ok, State#state{pending = [Message | Pending]}};
handle_send(Message, State = #state{broker_mapping = Mapping, pending = Pending,
                                    known_topics = KnownTopics}) ->
  {Topic, Partition, Payload} = Message,
  case lists:any(fun({K, _}) -> K =:= {Topic, Partition} end, Mapping) of
    true ->
      % The ets takes some time to be available after being created, so we check
      % if the topic/partition pair is in the mapping and if it does, we know we
      % just need to send it again. The order is not guaranteed in this case, so
      % if that's a concern, don't rely on autocreate_topics (besides, don't use
      % autocreate_topics on production since it opens another can of worms).
      ok = send(self(), Message, []),
      {reply, ok, State};
    false ->
      % However, if the topic/partition pair does not exist, we need to check if
      % the topic exists. If the topic exists, we drop the message because kafka
      % can't add partitions on the fly.
      case lists:any(fun({{T, _}, _}) -> T =:= Topic end, Mapping) of
        true ->
          ok = ?LOG_ERROR("Dropped ~p sent to topic ~p, partition ~p",
                          [Payload, Topic, Partition]),
          {reply, {error, bad_partition}, State};
        false ->
          NewKnownTopics = lists:umerge([Topic], KnownTopics),
          NewState = State#state{pending = [Message | Pending]},
          {reply, ok, handle_request_metadata(NewState, NewKnownTopics)}
      end
  end.

handle_fetch(_Topic, _Partition, _Options, #state{broker_mapping = void}) ->
  {error, not_connected};
handle_fetch(Topic, Partition, Options, State) ->
  case lists:keyfind({Topic, Partition}, 1, State#state.broker_mapping) of
    false ->
      {error, {no_broker, {Topic, Partition}}};
    {_, Broker} ->
      NewOptions = Options ++ State#state.default_fetch_options,
      kafkerl_broker_connection:fetch(Broker, Topic, Partition, NewOptions)
  end.

handle_stop_fetch(_Topic, _Partition, #state{broker_mapping = void}) ->
  % Ignore, there's no fetch in progress
  ok;
handle_stop_fetch(Topic, Partition, State) ->
  case lists:keyfind({Topic, Partition}, 1, State#state.broker_mapping) of
    false ->
      % Ignore, there's no fetch in progress
      ok;
    {_, Broker} ->
      kafkerl_broker_connection:stop_fetch(Broker, Topic, Partition)
  end.

handle_get_partitions(#state{broker_mapping = void}) ->
  {error, not_available};
handle_get_partitions(#state{broker_mapping = Mapping}) ->
  {ok, Mapping}.

handle_request_metadata(State, Topics) ->
  handle_request_metadata(State, Topics, false).

% Ignore it if the topic mapping is void, we are already requesting the metadata
handle_request_metadata(State = #state{broker_mapping = void}, _, false) ->
  State;
handle_request_metadata(State, NewTopics, _) ->
  SortedNewTopics = lists:sort(NewTopics),
  NewKnownTopics = lists:umerge(State#state.known_topics, SortedNewTopics),
  Now = get_timestamp(),
  LastRequest = State#state.last_metadata_request,
  Cooldown = State#state.metadata_request_cd,
  _ = case Cooldown - (Now - LastRequest) of
        Negative when Negative =< 0 ->
          _ = make_metadata_request(State);
        Time ->
          _ = timer:apply_after(Time, ?MODULE, request_metadata, [self(), true])
      end,
  State#state{broker_mapping = void, known_topics = NewKnownTopics,
              last_metadata_request = Now}.

%%==============================================================================
%% Utils
%%==============================================================================
get_ets_dump_name({OldName, Counter}) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:local_time(),
  Ts = io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B_",
                     [Year, Month, Day, Hour, Minute, Second]),
  PartialNewName = "kafkerl_messages_" ++ lists:flatten(Ts),
  case lists:prefix(PartialNewName, OldName) of
    true ->
      {PartialNewName ++ integer_to_list(Counter + 1) ++ ".dump", Counter + 1};
    _ ->
      {PartialNewName ++ "0.dump", 0}
  end.

get_metadata_tcp_options() ->
  kafkerl_utils:get_tcp_options([{active, false}, {packet, 4}]).

-spec do_request_metadata(pid(), [address()],
                                  any(),
                                  non_neg_integer(),
                                  non_neg_integer(),
                                  iodata()) ->
  metadata_timeout | {metadata_updated, broker_mapping()}.
do_request_metadata(Pid, _Brokers, _TCPOpts, 0, _RetryInterval, _Request) ->
  Pid ! metadata_timeout;
do_request_metadata(Pid, Brokers, TCPOpts, Retries, RetryInterval, Request) ->
  case do_request_metadata(Brokers, TCPOpts, Request) of
    {ok, TopicMapping} ->
      Pid ! {metadata_updated, TopicMapping};
    _Error ->
      timer:sleep(RetryInterval),
      NewRetries = case Retries of
                     -1 -> -1;
                     N  -> N - 1
                   end,
      do_request_metadata(Pid, Brokers, TCPOpts, NewRetries, RetryInterval,
                       Request)
  end.

do_request_metadata([], _TCPOpts, _Request) ->
  {error, all_down};
do_request_metadata([{Host, Port} = _Broker | T], TCPOpts, Request) ->
  ok = ?LOG_DEBUG("Attempting to connect to broker at ~s:~p", [Host, Port]),
  % Connect to the Broker
  case gen_tcp:connect(Host, Port, TCPOpts) of
    {error, Reason} ->
      warn_metadata_request(Host, Port, Reason),
      % Failed, try with the next one in the list
      do_request_metadata(T, TCPOpts, Request);
    {ok, Socket} ->
      % On success, send the metadata request
      case gen_tcp:send(Socket, Request) of
        {error, Reason} ->
          warn_metadata_request(Host, Port, Reason),
          % Unable to send request, try the next broker
          do_request_metadata(T, TCPOpts, Request);
        ok ->
          case gen_tcp:recv(Socket, 0, 6000) of
            {error, Reason} ->
              warn_metadata_request(Host, Port, Reason),
              gen_tcp:close(Socket),
              % Nothing received (probably a timeout), try the next broker
              do_request_metadata(T, TCPOpts, Request);
            {ok, Data} ->
              gen_tcp:close(Socket),
              parse_metadata_response(Data, Host, Port, T, TCPOpts,
                                      Request)
          end
      end
  end.

parse_metadata_response(Data, Host, Port, T, TCPOpts, Request) ->
  case kafkerl_protocol:parse_metadata_response(Data) of
    {error, Reason} ->
      warn_metadata_request(Host, Port, Reason),
      % The parsing failed, try the next broker
      do_request_metadata(T, TCPOpts, Request);
    {ok, _CorrelationId, Metadata} ->
      % We received a metadata response, make sure it has brokers
      {ok, get_topic_mapping(Metadata)}
  end.

send_event(Event, {all, Callback}) ->
  kafkerl_utils:send_event(Callback, Event);
send_event({EventName, _Data} = Event, {Events, Callback}) ->
  case is_list(Events) andalso lists:member(EventName, Events) of
    true -> kafkerl_utils:send_event(Callback, Event);
    false -> ok
  end;
send_event(Event, Callbacks) ->
  lists:filter(fun(Callback) ->
                 send_event(Event, Callback) =:= ok
               end, Callbacks).

%%==============================================================================
%% Request building
%%==============================================================================
metadata_request(#state{client_id = ClientId}, [] = _NewTopics) ->
  kafkerl_protocol:build_metadata_request([], 0, ClientId);
metadata_request(#state{known_topics = KnownTopics, client_id = ClientId},
                 NewTopics) ->
  AllTopics = lists:umerge(KnownTopics, NewTopics),
  kafkerl_protocol:build_metadata_request(AllTopics, 0, ClientId).

%%==============================================================================
%% Topic/broker mapping
%%==============================================================================
get_topic_mapping({BrokerMetadata, TopicMetadata}) ->
  % Converts [{ErrorCode, Topic, [Partion]}] to [{Topic, [Partition]}]
  Topics = lists:filtermap(fun expand_topic/1, TopicMetadata),
  % Converts [{Topic, [Partition]}] on [{{Topic, Partition}, BrokerId}]
  Partitions = lists:flatten(lists:filtermap(fun expand_partitions/1, Topics)),
  % Converts the BrokerIds from the previous array into socket addresses
  lists:filtermap(fun({{Topic, Partition}, BrokerId}) ->
                    case lists:keyfind(BrokerId, 1, BrokerMetadata) of
                      {BrokerId, HostData} ->
                        {true, {{Topic, Partition, BrokerId}, HostData}};
                      _Any ->
                        false
                    end
                  end, Partitions).

expand_topic({?NO_ERROR, Topic, Partitions}) ->
  {true, {Topic, Partitions}};
expand_topic({Error = ?REPLICA_NOT_AVAILABLE, Topic, Partitions}) ->
  % Replica not available can be ignored, still, show a warning
  ok = ?LOG_WARNING("Ignoring ~p on metadata for topic ~p",
                    [kafkerl_error:get_error_name(Error), Topic]),
  {true, {Topic, Partitions}};
expand_topic({Error, Topic, _Partitions}) ->
  ok = ?LOG_ERROR("Error ~p on metadata for topic ~p",
                  [kafkerl_error:get_error_name(Error), Topic]),
  {true, {Topic, []}}.

expand_partitions(Metadata) ->
  expand_partitions(Metadata, []).

expand_partitions({_Topic, []}, Acc) ->
  {true, Acc};
expand_partitions({Topic, [{?NO_ERROR, Partition, Leader, _, _} | T]}, Acc) ->
  ExpandedPartition = {{Topic, Partition}, Leader},
  expand_partitions({Topic, T}, [ExpandedPartition | Acc]);
expand_partitions({Topic, [{Error = ?REPLICA_NOT_AVAILABLE, Partition, Leader,
                            _, _} | T]}, Acc) ->
  ok = ?LOG_WARNING("Ignoring ~p on metadata for topic ~p, partition ~p",
                    [kafkerl_error:get_error_name(Error), Topic, Partition]),
  ExpandedPartition = {{Topic, Partition}, Leader},
  expand_partitions({Topic, T}, [ExpandedPartition | Acc]);
expand_partitions({Topic, [{Error, Partition, _, _, _} | T]}, Acc) ->
  ok = ?LOG_ERROR("Error ~p on metadata for topic ~p, partition ~p",
                  [kafkerl_error:get_error_name(Error), Topic, Partition]),
  expand_partitions({Topic, T}, Acc).

get_broker_mapping(TopicMapping, State) ->
  get_broker_mapping(TopicMapping, State, 0, []).

get_broker_mapping([], _State, _N, Acc) ->
  [{Key, Address} || {_ConnId, Key, Address} <- Acc];
get_broker_mapping([{{Topic, Partition, ConnId}, Address} | T],
                   State = #state{config = Config}, N, Acc) ->
  Buffer = kafkerl_utils:buffer_name(Topic, Partition),
  _ = ets_buffer:create(Buffer, fifo),
  {Conn, NewN} = case lists:keyfind(ConnId, 1, Acc) of
                   false ->
                     {start_broker_connection(N, Address, Config), N + 1};
                   {ConnId, _, BrokerConnection} ->
                     {BrokerConnection, N}
                 end,

  Buffer = kafkerl_utils:buffer_name(Topic, Partition),
  _ = ets_buffer:create(Buffer, fifo),
  kafkerl_broker_connection:add_buffer(Conn, Buffer),

  NewMapping = {ConnId, {Topic, Partition}, Conn},
  get_broker_mapping(T, State, NewN, [NewMapping | Acc]).

start_broker_connection(N, Address, Config) ->
  case kafkerl_broker_connection:start_link(N, self(), Address, Config) of
    {ok, Name, _Pid} ->
      Name;
    {error, {already_started, Pid}} ->
      kafkerl_broker_connection:clear_buffers(Pid),
      Pid
  end.

% This is used to return the available partitions for each topic
get_partitions_from_mapping(Mapping) ->
  F = fun({{Topic, Partition}, _}, Acc) ->
        case lists:keytake(Topic, 1, Acc) of
          false ->
            [{Topic, [Partition]} | Acc];
          {value, {Topic, Partitions}, NewAcc} ->
            [{Topic, [Partition | Partitions]} | NewAcc]
        end
      end,
  lists:foldl(F, [], Mapping).

send_mapping_to(_NewCallback, #state{broker_mapping = void}) ->
  ok;
send_mapping_to(NewCallback, #state{broker_mapping = Mapping}) ->
  Partitions = get_partitions_from_mapping(Mapping),
  send_event({partition_update, Partitions}, NewCallback).

-spec make_metadata_request(state()) -> {pid(), reference()}.
make_metadata_request(State = #state{brokers = Brokers,
                                     known_topics = Topics,
                                     max_metadata_retries = MaxMetadataRetries,
                                     retry_interval = RetryInterval}) ->
  Request = metadata_request(State, Topics),
  % Start requesting metadata
  Params = [self(), Brokers, get_metadata_tcp_options(), MaxMetadataRetries,
            RetryInterval, Request],
  spawn_monitor(?MODULE, do_request_metadata, Params).

get_timestamp() ->
  {A, B, C} = erlang:timestamp(),
  (A * 1000000 + B) * 1000 + C div 1000.

%%==============================================================================
%% Error handling
%%==============================================================================
warn_metadata_request(Host, Port, Reason) ->
  ok = ?LOG_WARNING("Unable to retrieve metadata from ~s:~p, reason: ~p",
                    [Host, Port, Reason]).