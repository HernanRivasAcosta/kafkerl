-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
% Metadata
-export([request_metadata/1, request_metadata/2, request_metadata/3,
         get_partitions/1]).
% Produce
-export([send/1]).
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
                config                = [] :: {atom(), any()},
                autocreate_topics  = false :: boolean(),
                callbacks             = [] :: [{filters(), kafkerl:callback()}],
                known_topics          = [] :: [binary()],
                last_dump_name   = {"", 0} :: {string(), integer()},
                default_fetch_options = [] :: kafkerl:options(),
                dump_location         = "" :: string(),
                max_buffer_size        = 0 :: integer(),
                save_bad_messages  = false :: boolean(),
                metadata_handler    = void :: atom()}).
-type state() :: #state{}.

-export_type([address/0]).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(atom(), any()) -> {ok, pid()} | ignore | kafkerl:error().
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config, Name], []).

-spec send(kafkerl:basic_message()) ->
  ok | kafkerl:error().
send({Topic, Partition, _Payload} = Message) ->
  Buffer = kafkerl_utils:buffer_name(Topic, Partition),
  case ets_buffer:write(Buffer, Message) of
    NewSize when is_integer(NewSize) ->
      ok;
    Error ->
      _ = lager:debug("unable to write on ~p, reason: ~p", [Buffer, Error]),
      case ets_buffer:write(kafkerl_utils:default_buffer_name(), Message) of
        NewDefaultBufferSize when is_integer(NewDefaultBufferSize) -> 
          ok;
        _ ->
          _ = lager:critical("unable to write to default buffer, reason: ~p",
                             [Error]),
          ok
      end
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

-spec produce_succeeded(kafkerl:server_ref(), [{kafkerl:topic(),
                                                kafkerl:partition(),
                                                [binary()],
                                                integer()}]) -> ok.
produce_succeeded(ServerRef, Messages) ->
  gen_server:cast(ServerRef, {produce_succeeded, Messages}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()} |
                                            {reply, {error, any()}, state()}.
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

-spec handle_info(any(), state()) -> {noreply, state()} |
                                     {stop, {error, any()}, state()}.
handle_info(dump_buffer_tick, State) ->
  {noreply, handle_dump_buffer_to_disk(State)};
handle_info(metadata_timeout, State) ->
  {stop, {error, unable_to_retrieve_metadata}, State};
handle_info({metadata_updated, []}, State) ->
  % If the metadata arrived empty request it again
  {noreply, handle_request_metadata(State#state{broker_mapping = []}, [])};
handle_info({metadata_updated, Mapping}, State) ->
  % Create the topic mapping (this also starts the broker connections)
  NewBrokerMapping = get_broker_mapping(Mapping, State),
  _ = lager:debug("Refreshed topic mapping: ~p", [NewBrokerMapping]),
  % Get the partition data to send to the subscribers and send it
  PartitionData = get_partitions_from_mapping(NewBrokerMapping),
  Callbacks = State#state.callbacks,
  NewCallbacks = send_event({partition_update, PartitionData}, Callbacks),
  % Add to the list of known topics
  NewTopics = lists:sort([T || {T, _P} <- PartitionData]),
  NewKnownTopics = lists:umerge(NewTopics, State#state.known_topics),
  _ = lager:debug("Known topics: ~p", [NewKnownTopics]),
  % TODO: Maybe retry from the dumps
  {noreply, State#state{broker_mapping = NewBrokerMapping,
                        callbacks = NewCallbacks,
                        known_topics = NewKnownTopics}};
handle_info({'DOWN', Ref, process, _, normal}, State) ->
  true = demonitor(Ref),
  {noreply, State};
handle_info({'DOWN', Ref, process, _, Reason}, State) ->
  _ = lager:error("metadata request failed, reason: ~p", [Reason]),
  true = demonitor(Ref),
  {noreply, handle_request_metadata(State, [], true)};
handle_info(Msg, State) ->
  _ = lager:notice("Unexpected info message received: ~p on ~p", [Msg, State]),
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
init([Config, Name]) ->
  % The schema indicates what is expected of the configuration, it validates and
  % normalizes the configuration
  Schema = [{brokers, [{string, {integer, {1, 65535}}}], required},
            {client_id, binary, {default, <<"kafkerl_client">>}},
            {topics, [binary], required},
            {assume_autocreate_topics, boolean, {default, false}},
            {consumer_min_bytes, positive_integer, {default, 1}},
            {consumer_max_wait, positive_integer, {default, 1500}},
            {dump_location, string, {default, ""}},
            {max_buffer_size, positive_integer, {default, 500}},
            {save_messages_for_bad_topics, boolean, {default, true}},
            {flush_to_disk_every, positive_integer, {default, 10000}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [Brokers, ClientId, Topics, AutocreateTopics, MinBytes, MaxWait,
          DumpLocation, MaxBufferSize, SaveBadMessages, FlushToDiskInterval]} ->
      % Start the metadata request handler
      MetadataHandlerName = metadata_handler_name(Name),
      {ok, _} = kafkerl_metadata_handler:start(MetadataHandlerName, Config),
      % Build the default fetch options
      DefaultFetchOptions = [{min_bytes, MinBytes}, {max_wait, MaxWait}],
      State = #state{config                = Config,
                     known_topics          = Topics,
                     brokers               = Brokers,
                     client_id             = ClientId,
                     dump_location         = DumpLocation,
                     max_buffer_size       = MaxBufferSize,
                     save_bad_messages     = SaveBadMessages,
                     autocreate_topics     = AutocreateTopics,
                     default_fetch_options = DefaultFetchOptions,
                     metadata_handler      = MetadataHandlerName},
      % Create a buffer to hold unsent messages
      _ = ets_buffer:create(kafkerl_utils:default_buffer_name(), fifo),
      % Start the interval that manages the buffers holding unsent messages
      {ok, _TRef} = timer:send_interval(FlushToDiskInterval, dump_buffer_tick),
      {_Pid, _Ref} = make_metadata_request(State),
      {ok, State};
    {errors, Errors} ->
      ok = lists:foreach(fun(E) ->
                           _ = lager:critical("Connector config error ~p", [E])
                         end, Errors),
      {stop, bad_config}
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
  LastMetadataUpdate = case Cooldown - (Now - LastRequest) of
        Negative when Negative =< 0 ->
          _ = make_metadata_request(State),
          Now;
        Time ->
          _ = timer:apply_after(Time, ?MODULE, request_metadata, [self(), true]),
          LastRequest
      end,
  State#state{broker_mapping = void, known_topics = NewKnownTopics,
              last_metadata_request = LastMetadataUpdate}.

handle_dump_buffer_to_disk(State = #state{dump_location = DumpLocation,
                                          last_dump_name = LastDumpName}) ->
  % Get the buffer name and all the messages from it
  Buffer = kafkerl_utils:default_buffer_name(),
  MessagesInBuffer = ets_buffer:read_all(Buffer),
  % Split them between the ones that should be retried and those that don't
  {ToDump, ToRetry} = split_message_dump(MessagesInBuffer, State),
  % Retry the messages on an async function (to avoid locking this gen_server)
  ok = retry_messages(ToRetry),
  % And dump the messages that need to be dumped into a file
  case ToDump of
    [_ | _] = Messages ->
      % Get the name of the file we want to write to
      {DumpNameStr, _} = NewDumpName = get_ets_dump_name(LastDumpName),
      % Build the location
      WorkingDirectory = case file:get_cwd() of
                           {ok, Path} -> Path;
                           {error, _} -> ""
                         end,
      FilePath = filename:join([WorkingDirectory, DumpLocation, DumpNameStr]),
      % Write to disk
      _ = case file:write_file(FilePath, term_to_binary(Messages)) of
            ok ->
              lager:info("Dumped unsent messages at ~p", [FilePath]);
            Error ->
              lager:critical("Unable to save messages, reason: ~p", [Error])
          end,
      State#state{last_dump_name = NewDumpName};
    _ ->
      State
  end.

%%==============================================================================
%% Utils
%%==============================================================================
retry_messages([]) ->
  ok;
retry_messages(Messages) ->
  _Pid = spawn(fun() -> [send(M) || M <- Messages] end),
  ok.

split_message_dump(Messages, #state{known_topics = KnownTopics,
                                    max_buffer_size = MaxBufferSize,
                                    save_bad_messages = SaveBadMessages})
  when is_list(Messages) ->

  % Split messages between for topics kafkerl knows exist and those that do not.
  {Known, Unknown} = lists:partition(fun({Topic, _Partition, _Payload}) ->
                                       lists:member(Topic, KnownTopics)
                                     end, Messages),
  % The messages to be dumped are those from unkown topics (if the settings call
  % for it) and those from known topics if the buffer size is too large.
  % The messages to be retried are those from the known topics, as long as their
  % number does not exceed the MaxBufferSize.
  case {SaveBadMessages, length(Known) >= MaxBufferSize} of
    {true, true} ->
      {Unknown ++ Known, []};
    {false, true} ->
      {Known, []};
    {true, false} ->
      {Unknown, Known};
    {false, false} ->
      {[], Known}
  end;
% If the messages are not a list, then it's an ets error, report it and move on.
% And yes, those messages are gone forever
split_message_dump(Error, _State) ->
  lager:error("Unable to get messages from buffer, reason: ~p", [Error]),
  {[], []}.

get_ets_dump_name({OldName, Counter}) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:local_time(),
  Ts = io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B_",
                     [Year, Month, Day, Hour, Minute, Second]),
  PartialNewName = "kafkerl_messages_" ++ lists:flatten(Ts),
  case lists:prefix(PartialNewName, OldName) of
    true ->
      {PartialNewName ++ integer_to_list(Counter) ++ ".dump", Counter + 1};
    _ ->
      {PartialNewName ++ "0.dump", 1}
  end.

get_metadata_tcp_options() ->
  kafkerl_utils:get_tcp_options([{active, false}, {packet, 4}]).

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
  _ = lager:debug("Attempting to connect to broker at ~s:~p", [Host, Port]),
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
              case kafkerl_protocol:parse_metadata_response(Data) of
                {error, Reason} ->
                  warn_metadata_request(Host, Port, Reason),
                  % The parsing failed, try the next broker
                  do_request_metadata(T, TCPOpts, Request);
                {ok, _CorrelationId, Metadata} ->
                  % We received a metadata response, make sure it has brokers
                  {ok, get_topic_mapping(Metadata)}
              end
          end
      end
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

metadata_handler_name(ServerName) ->
  list_to_binary([atom_to_list(ServerName), "_metadata_handler"]).

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
  _ = lager:warning("Ignoring ~p on metadata for topic ~p",
                    [kafkerl_error:get_error_name(Error), Topic]),
  {true, {Topic, Partitions}};
expand_topic({Error, Topic, _Partitions}) ->
  _ = lager:error("Error ~p on metadata for topic ~p",
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
  _ = lager:warning("Ignoring ~p on metadata for topic ~p, partition ~p",
                    [kafkerl_error:get_error_name(Error), Topic, Partition]),
  ExpandedPartition = {{Topic, Partition}, Leader},
  expand_partitions({Topic, T}, [ExpandedPartition | Acc]);
expand_partitions({Topic, [{Error, Partition, _, _, _} | T]}, Acc) ->
  _ = lager:error("Error ~p on metadata for topic ~p, partition ~p",
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
  _ = lager:warning("Unable to retrieve metadata from ~s:~p, reason: ~p",
                    [Host, Port, Reason]).