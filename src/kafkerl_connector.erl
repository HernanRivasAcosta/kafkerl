-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([send/3, request_metadata/1, request_metadata/2, request_metadata/3,
         subscribe/2, subscribe/3, get_partitions/1, unsubscribe/2]).
% Only for broker connections
-export([produce_succeeded/2]).
% Supervisors
-export([start_link/2]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

-type server_ref()          :: atom() | pid().
-type broker_mapping_key()  :: {topic(), partition()}.
-type broker_mapping()      :: {broker_mapping_key(), server_ref()}.

-record(state, {brokers                 = [] :: [socket_address()],
                broker_mapping        = void :: [broker_mapping()] | void,
                client_id             = <<>> :: client_id(),
                max_metadata_retries    = -1 :: integer(),
                retry_interval           = 1 :: non_neg_integer(),
                config                  = [] :: {atom(), any()},
                autocreate_topics    = false :: boolean(),
                callbacks               = [] :: [{filters(), callback()}],
                known_topics            = [] :: [binary()],
                pending                 = [] :: [basic_message()],
                metadata_request_cd      = 0 :: integer(),
                last_dump_name     = {"", 0} :: {string(), integer()}}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(atom(), any()) -> {ok, pid()} | ignore | error().
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config], []).

-spec send(server_ref(), basic_message(), kafkerl:produce_option()) ->
  ok | error().
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
      lager:debug("unable to send message to ~p, reason: ~p", [Buffer, Error]),
      gen_server:call(ServerRef, {send, Message})
  end.

-spec get_partitions(server_ref()) -> [{topic(), [partition()]}] | error().
get_partitions(ServerRef) ->
  case gen_server:call(ServerRef, {get_partitions}) of
    {ok, Mapping} ->
      get_partitions_from_mapping(Mapping);
    Error ->
      Error
  end.

-spec subscribe(server_ref(), callback()) -> ok | error().
subscribe(ServerRef, Callback) ->
  subscribe(ServerRef, Callback, all).
-spec subscribe(server_ref(), callback(), filters()) -> ok | error().
subscribe(ServerRef, Callback, Filter) ->
  gen_server:call(ServerRef, {subscribe, {Filter, Callback}}).
-spec unsubscribe(server_ref(), callback()) -> ok.
unsubscribe(ServerRef, Callback) ->
  gen_server:call(ServerRef, {unsubscribe, Callback}).

-spec request_metadata(server_ref()) -> ok.
request_metadata(ServerRef) ->
  gen_server:call(ServerRef, {request_metadata}).

-spec request_metadata(server_ref(), [topic()] | boolean()) -> ok.
request_metadata(ServerRef, TopicsOrForced) ->
  gen_server:call(ServerRef, {request_metadata, TopicsOrForced}).

-spec request_metadata(server_ref(), [topic()], boolean()) -> ok.
request_metadata(ServerRef, Topics, Forced) ->
  gen_server:call(ServerRef, {request_metadata, Topics, Forced}).

-spec produce_succeeded(server_ref(),
                        [{topic(), partition(), [binary()], integer()}]) -> ok.
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
  ok = case file:write_file(FilePath, term_to_binary(AllMessages)) of
         ok    -> lager:debug("Dumped unsent messages at ~p", [FilePath]);
         Error -> lager:critical("Unable to save messages, reason: ~p", [Error])
       end,
  {reply, ok, State#state{last_dump_name = DumpName}};
handle_call({send, Message}, _From, State) ->
  handle_send(Message, State);
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

handle_info(metadata_timeout, State) ->
  {stop, {error, unable_to_retrieve_metadata}, State};
handle_info({metadata_updated, []}, State) ->
  % If the metadata arrived empty request it again
  {noreply, handle_request_metadata(State#state{broker_mapping = []}, [])};
handle_info({metadata_updated, Mapping}, State) ->
  % Create the topic mapping (this also starts the broker connections)
  NewBrokerMapping = get_broker_mapping(Mapping, State),
  lager:debug("Refreshed topic mapping: ~p", [NewBrokerMapping]),
  % Get the partition data to send to the subscribers and send it
  PartitionData = get_partitions_from_mapping(NewBrokerMapping),
  Callbacks = State#state.callbacks,
  NewCallbacks = send_event({partition_update, PartitionData}, Callbacks),
  % Add to the list of known topics
  NewTopics = lists:sort([T || {T, _P} <- PartitionData]),
  NewKnownTopics = lists:umerge(NewTopics, State#state.known_topics),
  lager:debug("Known topics: ~p", [NewKnownTopics]),
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
  lager:error("metadata request failed, reason: ~p", [Reason]),
  true = demonitor(Ref),
  {noreply, handle_request_metadata(State, [], true)};
handle_info(Msg, State) ->
  lager:notice("Unexpected info message received: ~p on ~p", [Msg, State]),
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
init([Config]) ->
  Schema = [{brokers, [{string, {integer, {1, 65535}}}], required},
            {max_metadata_retries, {integer, {-1, undefined}}, {default, -1}},
            {client_id, binary, {default, <<"kafkerl_client">>}},
            {topics, [binary], required},
            {metadata_tcp_timeout, positive_integer, {default, 1500}},
            {assume_autocreate_topics, boolean, {default, false}},
            {metadata_request_cooldown, positive_integer, {default, 333}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [Brokers, MaxMetadataRetries, ClientId, Topics, RetryInterval,
          AutocreateTopics, MetadataRequestCooldown]} ->
      State = #state{config               = Config,
                     known_topics         = Topics,
                     brokers              = Brokers,
                     client_id            = ClientId,
                     retry_interval       = RetryInterval,
                     autocreate_topics    = AutocreateTopics,
                     max_metadata_retries = MaxMetadataRetries,
                     metadata_request_cd  = MetadataRequestCooldown},
      {ok, _Pid} = kafkerl_metadata_requester:start_link(),
      erlang:send_after(0, self(), {metadata_updated, []}),
      {ok, State};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("Connector config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_send(Message, State = #state{autocreate_topics = false}) ->
  % The topic didn't exist, ignore
  {Topic, _Partition, Payload} = Message,
  lager:error("Dropping ~p sent to non existing topic ~p", [Payload, Topic]),
  {reply, ok, State};
handle_send(Message, State = #state{broker_mapping = void,
                                    pending = Pending}) ->
  % Maybe have a new buffer
  {reply, ok, State#state{pending = [Message | Pending]}};
handle_send(Message, State = #state{broker_mapping = Mapping, pending = Pending,
                                    known_topics = KnownTopics}) ->
  {Topic, Partition, Payload} = Message,
  case lists:any(fun({K, _}) -> K =:= {Topic, Partition} end, Mapping) of
    true ->
      % We need to check if the topic/partition pair exists, this is because the
      % ets takes some time to start, so some messages could be lost.
      % Therefore if we have the topic/partition, just send it again (the order
      % will suffer though)
      send(self(), Message, []),
      {reply, ok, State};
    false ->
      % Now, if the topic/partition was not valid, we need to check if the topic
      % exists, if it does, just drop the message as we can assume no partitions
      % are created.
      case lists:any(fun({{T, _}, _}) -> T =:= Topic end, Mapping) of
        true ->
          lager:error("Dropping ~p sent to topic ~p, partition ~p",
                      [Payload, Topic, Partition]),
          {reply, ok, State};
        false ->
          NewKnownTopics = lists:umerge([Topic], KnownTopics),
          NewState = State#state{pending = [Message | Pending]},
          {reply, ok, handle_request_metadata(NewState, NewKnownTopics)}
      end
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
handle_request_metadata(State = #state{brokers = Brokers,
                                       known_topics = Topics,
                                       max_metadata_retries = MaxMetadataRetries,
                                       retry_interval = RetryInterval},
                                 NewTopics, _) ->
  SortedNewTopics = lists:sort(NewTopics),
  NewKnownTopics = lists:umerge(State#state.known_topics, SortedNewTopics),
  Request = metadata_request(State, Topics),
  % Start requesting metadata
  BrokerMapping = case kafkerl_metadata_requester:req_metadata(Brokers, get_metadata_tcp_options(), MaxMetadataRetries,
    RetryInterval, Request) of
    metadata_timeout -> void;
    {error, all_down} -> void;
    {metadata_updated, Mapping} -> Mapping
  end,
  State#state{broker_mapping = BrokerMapping, known_topics = NewKnownTopics}.

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

