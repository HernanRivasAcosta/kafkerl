-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([send/2, send/3, request_metadata/1, get_partitions/1, subscribe/2,
         unsubscribe/2]).
% Only for internal use
-export([request_metadata/6]).
% Supervisors
-export([start_link/2]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

-type start_link_response() :: {ok, pid()} | ignore | error().

-type server_ref()          :: atom() | pid().
-type broker_mapping_key()  :: {topic(), partition()}.
-type broker_mapping()      :: {broker_mapping_key(), server_ref()}.

-record(state, {brokers                 = [] :: [socket_address()],
                broker_mapping        = void :: [broker_mapping()] | void,
                client_id             = <<>> :: client_id(),
                topics                  = [] :: [topic()],
                max_metadata_retries    = -1 :: integer(),
                retry_interval           = 1 :: non_neg_integer(),
                config                  = [] :: {atom(), any()},
                retry_on_topic_error = false :: boolean(),
                callbacks               = [] :: [callback()]}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(atom(), any()) -> start_link_response().
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config], []).

-spec send(server_ref(), basic_message()) -> ok | error().
send(ServerRef, Message) when is_atom(ServerRef) ->
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

-spec subscribe(server_ref(), callback()) -> ok.
subscribe(ServerRef, Callback) ->
  gen_server:call(ServerRef, {subscribe, Callback}).
-spec unsubscribe(server_ref(), callback()) -> ok.
unsubscribe(ServerRef, Callback) ->
  gen_server:call(ServerRef, {unsubscribe, Callback}).

-spec request_metadata(server_ref()) -> ok.
request_metadata(ServerRef) ->
  gen_server:call(ServerRef, {request_metadata}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()} |
                                            {reply, {error, any()}, state()}.
handle_call({send, Message}, _From, State) ->
  {reply, handle_send(Message, State), State};
handle_call({request_metadata}, _From, State) ->
  {reply, ok, handle_request_metadata(State)};
handle_call({get_partitions}, _From, State) ->
  {reply, handle_get_partitions(State), State};
handle_call({subscribe, Callback}, _From, State) ->
  {reply, ok, State#state{callbacks = [Callback | State#state.callbacks]}};
handle_call({unsubscribe, Callback}, _From, State) ->
  {reply, ok, State#state{callbacks = State#state.callbacks -- [Callback]}}.

handle_info(metadata_timeout, State) ->
  {stop, {error, unable_to_retrieve_metadata}, State};
handle_info({metadata_updated, Mapping}, State) ->
  BrokerMapping = get_broker_mapping(Mapping, State),
  lager:debug("Refreshed topic mapping: ~p", [BrokerMapping]),
  PartitionData = get_partitions_from_mapping(BrokerMapping),
  NewCallbacks = lists:filter(fun(Callback) ->
                                kafkerl_utils:send_event(partition_update,
                                                         Callback,
                                                         PartitionData) =:= ok
                              end, State#state.callbacks),
  NewState = State#state{broker_mapping = BrokerMapping,
                         callbacks = NewCallbacks},
  {noreply, NewState};
handle_info(Msg, State) ->
  lager:notice("Unexpected info message received: ~p on ~p", [Msg, State]),
  {noreply, State}.

% Boilerplate
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
                     topics               = Topics,
                     brokers              = Brokers,
                     client_id            = ClientId,
                     retry_interval       = RetryInterval,
                     retry_on_topic_error = RetryOnTopicError,
                     max_metadata_retries = MaxMetadataRetries},
      Request = metadata_request(State),
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

handle_send(Message, #state{broker_mapping = Mapping}) ->
  {Topic, Partition, Payload} = Message,
  case lists:keyfind({Topic, Partition}, 1, Mapping) of
    false ->
      lager:error("Dropping ~p sent to topic ~p, partition ~p, reason: ~p",
                  [Payload, Topic, Partition, no_broker]),
      {error, invalid_topic_or_partition};
    {_, Broker} ->
      kafkerl_broker_connection:send(Broker, Message)
  end.

handle_get_partitions(#state{broker_mapping = void}) ->
  {error, not_available};
handle_get_partitions(#state{broker_mapping = Mapping}) ->
  {ok, Mapping}.

% Ignore it if the topic mapping is void, we are already requesting the metadata
handle_request_metadata(State = #state{broker_mapping = void}) ->
  State;
handle_request_metadata(State = #state{brokers = Brokers,
                                       retry_interval = RetryInterval,
                                       max_metadata_retries = MaxRetries}) ->
  Request = metadata_request(State),
  Params = [self(), Brokers, get_metadata_tcp_options(), MaxRetries,
            RetryInterval, Request],
  _Pid = spawn_link(?MODULE, request_metadata, Params),
  State#state{broker_mapping = void}.

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

%%==============================================================================
%% Request building
%%==============================================================================
metadata_request(#state{topics = Topics, client_id = ClientId}) ->
  kafkerl_protocol:build_metadata_request(Topics, 0, ClientId).

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
  false.

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

%%==============================================================================
%% Error handling
%%==============================================================================
warn_metadata_request(Host, Port, Reason) ->
  lager:warning("Unable to retrieve metadata from ~s:~p, reason: ~p",
                [Host, Port, Reason]).