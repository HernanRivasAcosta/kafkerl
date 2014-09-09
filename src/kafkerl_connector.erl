-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([send/2, send/3, request_metadata/1, request_metadata/2, subscribe/2,
         subscribe/3, get_partitions/1, unsubscribe/2]).
% Only for internal use
-export([request_metadata/6]).
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
                retry_on_topic_error = false :: boolean(),
                callbacks               = [] :: [{filters(), callback()}],
                known_topics            = [] :: [binary()],
                pending                 = [] :: [basic_message()]}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(atom(), any()) -> {ok, pid()} | ignore | error().
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config], []).

-spec send(server_ref(), basic_message()) -> ok | error().
send(ServerRef, Message) ->
  send(ServerRef, Message, 1000).
-spec send(server_ref(), basic_message(), integer() | infinity) -> ok | error().
send(ServerRef, Message, Timeout) ->
  gen_server:call(ServerRef, {send, Message}, Timeout).

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

-spec request_metadata(server_ref(), [topic()]) -> ok.
request_metadata(ServerRef, Topics) ->
  gen_server:call(ServerRef, {request_metadata, Topics}).

-spec produce_succeeded(server_ref(),
                        [{topic(), partition(), [binary()], integer()}]) -> ok.
produce_succeeded(ServerRef, Messages) ->
  gen_server:cast(ServerRef, {produce_succeeded, Messages}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()} |
                                            {reply, {error, any()}, state()}.
handle_call({send, Message}, _From, State) ->
  handle_send(Message, State);
handle_call({request_metadata}, _From, State) ->
  {reply, ok, handle_request_metadata(State, [])};
handle_call({request_metadata, Topics}, _From, State) ->
  {reply, ok, handle_request_metadata(State, Topics)};
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
  {reply, ok, State#state{callbacks = State#state.callbacks -- [Callback]}}.

handle_info(metadata_timeout, State) ->
  {stop, {error, unable_to_retrieve_metadata}, State};
handle_info({metadata_updated, []}, State) ->
  % If the metadata arrived empty request it again
  {noreply, handle_request_metadata(State, [])};
handle_info({metadata_updated, Mapping}, State = #state{pending = Pending}) ->
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
  NewState = State#state{broker_mapping = NewBrokerMapping,
                         callbacks = NewCallbacks,
                         known_topics = NewKnownTopics,
                         pending = []},
  F = fun(P) -> handle_send(P, NewState) end,
  ok = lists:foreach(F, Pending),
  {noreply, NewState};
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
            {metadata_tcp_timeout, {integer, {1, undefined}}, {default, 1500}},
            {retry_on_topic_error, boolean, {default, false}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [Brokers, MaxMetadataRetries, ClientId, Topics, RetryInterval,
          RetryOnTopicError]} ->
      State = #state{config               = Config,
                     known_topics         = Topics,
                     brokers              = Brokers,
                     client_id            = ClientId,
                     retry_interval       = RetryInterval,
                     retry_on_topic_error = RetryOnTopicError,
                     max_metadata_retries = MaxMetadataRetries},
      Request = metadata_request(State, Topics),
      % Start requesting metadata
      Params = [self(), Brokers, get_metadata_tcp_options(), MaxMetadataRetries,
                RetryInterval, Request],
      _Pid = spawn_link(?MODULE, request_metadata, Params),
      {ok, State};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("Connector config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_send(Message, State = #state{broker_mapping = void,
                                    pending = Pending}) ->
  % If we are waiting for the metadata, just save the message and move on
  % TODO: Using the buffer instead of a list in the gen_server will be safer
  {reply, ok, State#state{pending = [Message | Pending]}};
handle_send(Message, State = #state{broker_mapping = Mapping,
                                    known_topics = KnownTopics,
                                    retry_on_topic_error = RetryTopics}) ->
  {Topic, Partition, Payload} = Message,
  _ = case {lists:keyfind({Topic, Partition}, 1, Mapping), RetryTopics} of
        {false, false} ->
          % When retry topics is false, just fail
          lager:error("Dropping ~p sent to non existing topic ~p",
                      [Payload, Topic]),
          {error, invalid_topic};
        {false, true} ->
          % We need to check if the topic is valid, if it is and the partition
          % is invalid, then retrying is not going to work. Otherwise, we just
          % send the message to any broker as this will later on trigger a new
          % metadata request. There might be better ways of handling this, but
          % you should not be constantly sending messages to new topics anyway
          case lists:any(fun({{T, _}, _}) -> T =:= Topic end, Mapping) of
            true ->
              lager:error("Dropping ~p sent to topic ~p, partition ~p",
                          [Payload, Topic, Partition]),
              {error, invalid_topic};
            false ->
              [{_, Broker} | _] = Mapping,
              kafkerl_broker_connection:send(Broker, Message)
          end;
        {{_, Broker}, _} ->
          kafkerl_broker_connection:send(Broker, Message)
      end,
  NewKnownTopics = lists:umerge([Topic], KnownTopics),
  {reply, ok, State#state{known_topics = NewKnownTopics}}.

handle_get_partitions(#state{broker_mapping = void}) ->
  {error, not_available};
handle_get_partitions(#state{broker_mapping = Mapping}) ->
  {ok, Mapping}.

% Ignore it if the topic mapping is void, we are already requesting the metadata
handle_request_metadata(State = #state{broker_mapping = void}, _Topics) ->
  State;
handle_request_metadata(State, NewTopics) ->
  SortedNewTopics = lists:sort(NewTopics),
  NewKnownTopics = lists:umerge(State#state.known_topics, SortedNewTopics),
  Request = metadata_request(State, NewKnownTopics),
  Params = [self(), State#state.brokers, get_metadata_tcp_options(),
            State#state.max_metadata_retries, State#state.retry_interval,
            Request],
  _Pid = spawn_link(?MODULE, request_metadata, Params),
  State#state{broker_mapping = void, known_topics = NewKnownTopics}.

%%==============================================================================
%% Utils
%%==============================================================================
get_metadata_tcp_options() ->
  kafkerl_utils:get_tcp_options([{active, false}]).

request_metadata(Pid, _Brokers, _TCPOpts, 0, _RetryInterval, _Request) ->
  Pid ! metadata_timeout;
request_metadata(Pid, Brokers, TCPOpts, Retries, RetryInterval, Request) ->
  case request_metadata(Brokers, TCPOpts, Request) of
    {ok, TopicMapping} ->
      Pid ! {metadata_updated, TopicMapping};
    _Error ->
      timer:sleep(RetryInterval),
      NewRetries = case Retries of
                     -1 -> -1;
                     N  -> N - 1
                   end,
      request_metadata(Pid, Brokers, TCPOpts, NewRetries, RetryInterval,
                       Request)
  end.

request_metadata([], _TCPOpts, _Request) ->
  {error, all_down};
request_metadata([{Host, Port} = _Broker | T] = _Brokers, TCPOpts, Request) ->
  lager:debug("Attempting to connect to broker at ~s:~p", [Host, Port]),
  % Connect to the Broker
  case gen_tcp:connect(Host, Port, TCPOpts) of
    {error, Reason} ->
      warn_metadata_request(Host, Port, Reason),
      % Failed, try with the next one in the list
      request_metadata(T, TCPOpts, Request);
    {ok, Socket} ->
      % On success, send the metadata request
      case gen_tcp:send(Socket, Request) of
        {error, Reason} ->
          warn_metadata_request(Host, Port, Reason),
          % Unable to send request, try the next broker
          request_metadata(T, TCPOpts, Request);
        ok ->
          case gen_tcp:recv(Socket, 0, 6000) of
            {error, Reason} ->
              warn_metadata_request(Host, Port, Reason),
              gen_tcp:close(Socket),
              % Nothing received (probably a timeout), try the next broker
              request_metadata(T, TCPOpts, Request);
            {ok, Data} ->
              gen_tcp:close(Socket),
              case kafkerl_protocol:parse_metadata_response(Data) of
                {error, Reason} ->
                  warn_metadata_request(Host, Port, Reason),
                  % The parsing failed, try the next broker
                  request_metadata(T, TCPOpts, Request);
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

expand_topic({0, Topic, Partitions}) ->
  {true, {Topic, Partitions}};
expand_topic({Error, Topic, _Partitions}) ->
  lager:error("Error ~p on metadata for topic ~p",
              [kafkerl_error:get_error_name(Error), Topic]),
  {true, {Topic, []}}.

expand_partitions(Metadata) ->
  expand_partitions(Metadata, []).

expand_partitions({_Topic, []}, Acc) ->
  {true, Acc};
expand_partitions({Topic, [{0, Partition, Leader, _, _} | T]}, Acc) ->
  ExpandedPartition = {{Topic, Partition}, Leader},
  expand_partitions({Topic, T}, [ExpandedPartition | Acc]);
expand_partitions({Topic, [{Error, Partition, _, _, _} | T]}, Acc) ->
  lager:error("Error ~p on metadata for topic ~p, partition ~p",
              [kafkerl_error:get_error_name(Error), Topic, Partition]),
  expand_partitions({Topic, T}, Acc).

get_broker_mapping(TopicMapping, State) ->
  get_broker_mapping(TopicMapping, State, 0, []).

get_broker_mapping([], _State, _N, Acc) ->
  [{Key, Address} || {_ConnId, Key, Address} <- Acc];
get_broker_mapping([{{Topic, Partition, ConnId}, Address} | T],
                   State = #state{config = Config}, N, Acc) ->
  {Conn, NewN} = case lists:keyfind(ConnId, 1, Acc) of
                   false ->
                     {start_broker_connection(N, Address, Config), N + 1};
                   {ConnId, _, BrokerConnection} ->
                     {BrokerConnection, N}
                 end,
  NewMapping = {ConnId, {Topic, Partition}, Conn},
  get_broker_mapping(T, State, NewN, [NewMapping | Acc]).

start_broker_connection(N, Address, Config) ->
  case kafkerl_broker_connection:start_link(N, self(), Address, Config) of
    {ok, Name, _Pid} ->
      Name;
    {error, {already_started, Pid}} ->
      kafkerl_broker_connection:kill(Pid),
      start_broker_connection(N, Address, Config)
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

%%==============================================================================
%% Error handling
%%==============================================================================
warn_metadata_request(Host, Port, Reason) ->
  lager:warning("Unable to retrieve metadata from ~s:~p, reason: ~p",
                [Host, Port, Reason]).