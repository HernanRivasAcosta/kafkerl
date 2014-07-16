-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([send/1, send/2, request_metadata/0, request_metadata/1]).
% Only for internal use
-export([request_metadata/6]).
% Supervisors
-export([start_link/1, start_link/2]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type start_link_response() :: {ok, pid()} | ignore | error().

-type server_ref()          :: atom() | pid().
-type broker_mapping_key()   :: {topic(), partition()}.
-type broker_mapping()       :: {broker_mapping_key(), server_ref()}.

-record(state, {brokers              = [] :: [socket_address()],
                broker_mapping     = void :: [broker_mapping()] | void,
                client_id          = <<>> :: client_id(),
                topics               = [] :: [topic()],
                max_metadata_retries = -1 :: integer(),
                retry_interval        = 1 :: non_neg_integer(),
                config               = [] :: {atom(), any()}}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(any()) -> start_link_response().
start_link(Config) ->
  start_link(?MODULE, Config).
-spec start_link(atom(), any()) -> start_link_response().
start_link(undefined, Config) ->
  start_link(Config);
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config], []).

-spec send(basic_message()) -> ok | error().
send(Message) ->
  send(?MODULE, Message).
-spec send(server_ref(), basic_message()) -> ok | error();
          (binary(), integer() | infinity)  -> ok | error().
send(undefined, Message) ->
  send(?MODULE, Message, infinity);
send(ServerRef, Message) when is_atom(ServerRef) ->
  send(ServerRef, Message, 5000);
send(Message, Timeout) ->
  send(?MODULE, Message, Timeout).
-spec send(server_ref(), basic_message(), integer() | infinity) -> ok | error().
send(ServerRef, Message, Timeout) ->
  gen_server:call(ServerRef, {send, Message}, Timeout).

-spec request_metadata() -> ok.
request_metadata() ->
  request_metadata(?MODULE).
-spec request_metadata(server_ref()) -> ok.
request_metadata(ServerRef) ->
  gen_server:call(ServerRef, {request_metadata}).

% gen_server callbacks
-spec handle_call(any(), any(), state()) -> {reply, ok, state()} |
                                            {reply, {error, any()}, state()}.
handle_call({send, Message}, _From, State) ->
  {reply, handle_send(Message, State), State};
handle_call({request_metadata}, _From, State) ->
  {Ok, NewState} = handle_request_metadata(State),
  {reply, Ok, NewState}.

handle_info(metadata_timeout, State) ->
  {stop, {error, unable_to_retrieve_metadata}, State};
handle_info({metadata_updated, TopicMapping}, State) ->
  BrokerMapping = get_broker_mapping(TopicMapping, State),
  lager:info("refreshed topic mapping: ~p", [BrokerMapping]),
  {noreply, State#state{broker_mapping = BrokerMapping}};
handle_info(Msg, State) ->
  lager:warning("unexpected info message received: ~p on ~p", [Msg, State]),
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
            {metadata_tcp_timeout, {integer, {1, undefined}}, {default, 1500}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [Brokers, MaxMetadataRetries, ClientId, Topics, RetryInterval]} ->
      State = #state{brokers = Brokers,
                     topics = Topics,
                     client_id = ClientId,
                     max_metadata_retries = MaxMetadataRetries,
                     retry_interval = RetryInterval,
                     config = Config},
      Request = metadata_request(State),
      % Start requesting metadata
      Params = [self(), Brokers, get_metadata_tcp_options(), MaxMetadataRetries,
                RetryInterval, Request],
      _Pid = spawn_link(?MODULE, request_metadata, Params),
      {ok, State};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("connector config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_send(Message, #state{broker_mapping = Mapping}) ->
  {Topic, Partition, Payload} = Message,
  case lists:keyfind({Topic, Partition}, 1, Mapping) of
    false ->
      lager:error("dropping ~p sent to topic ~p, partition ~p, reason: ~p",
                  [Payload, Topic, Partition, no_broker]);
    {_, Broker} ->
      kafkerl_broker_connection:send(Broker, Message)
  end.

% If the topic mapping is void then we are already requesting the metadata
handle_request_metadata(State = #state{broker_mapping = void}) ->
  {ok, State};
handle_request_metadata(State = #state{brokers = Brokers,
                                       retry_interval = RetryInterval,
                                       max_metadata_retries = MaxRetries}) ->
  Request = metadata_request(State),
  Params = [self(), Brokers, get_metadata_tcp_options(), MaxRetries,
            RetryInterval, Request],
  _Pid = spawn_link(?MODULE, request_metadata, Params),
  {ok, State#state{broker_mapping = void}}.

%%==============================================================================
%% Utils
%%==============================================================================
get_tcp_options(Options) -> % TODO: refactor
  lists:ukeymerge(1, lists:sort(proplists:unfold(Options)), ?DEFAULT_TCP_OPTS).

get_metadata_tcp_options() ->
  get_tcp_options([{active, false}]).

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
  lager:error("error ~p on metadata for topic ~p",
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
  lager:error("error ~p on metadata for topic ~p, partition ~p",
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
  NewConn = kafkerl_broker_connection:start_link(N, self(), Address, Config),
  {ok, Name, _Pid} = NewConn,
  Name.

%%==============================================================================
%% Error handling
%%==============================================================================
warn_metadata_request(Host, Port, Reason) ->
  lager:warning("unable to retrieve metadata from ~s:~p, reason: ~p",
                [Host, Port, Reason]).