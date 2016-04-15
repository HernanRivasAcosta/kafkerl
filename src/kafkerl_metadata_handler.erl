-module(kafkerl_metadata_handler).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_fsm).

%% API
-export([request_metadata/1, get_known_topics/0]).
%% States
-export([idle/2, requesting/2, on_cooldown/2]).
%% Internal
-export([make_request/3]).
%% gen_fsm
-export([start_link/1, init/1, handle_info/3, terminate/3, code_change/4,
         handle_event/3, handle_sync_event/4]).

-include("kafkerl.hrl").

-record(state, {config             = [] :: [{atom(), any()}],
                client_id        = <<>> :: kafkerl_protocol:client_id(),
                brokers            = [] :: [kafkerl_connector:address()],
                max_retries        = -1 :: integer(),
                retry_interval      = 1 :: non_neg_integer(),
                cooldown            = 0 :: integer(),
                known_topics       = [] :: [kafkerl:topic()],
                next_topics        = [] :: [kafkerl:topic()]}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(any()) -> {ok, pid()} | ignore | kafkerl:error().
start_link(Config) ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, [Config], []).

-spec request_metadata([kafkerl:topic()]) -> ok.
request_metadata(Topics) ->
  gen_fsm:send_event(?MODULE, {request, Topics}).

-spec get_known_topics() -> ok.
get_known_topics() ->
  gen_fsm:sync_send_all_state_event(?MODULE, get_known_topics).

%%==============================================================================
%% States
%%==============================================================================
-spec idle(any(), state()) -> {next_state, atom(), state()}.
idle({request, Topics}, State = #state{known_topics = KnownTopics}) ->
  % Add the requested topics to the state
  SortedTopics = lists:usort(KnownTopics),
  NewKnownTopics = lists:umerge(Topics, SortedTopics),
  NewState = State#state{known_topics = NewKnownTopics},
  % Make the request
  ok = schedule_metadata_request(NewState),
  % And move the the requesting state
  {next_state, requesting, NewState}.

-spec requesting(any(), state()) -> {next_state, atom(), state()}.
% Handle a new metadata request while there's one in progress
requesting({request, NewTopics}, State = #state{known_topics = KnownTopics}) ->
  SortedTopics = lists:usort(NewTopics), % This also removes repeated entries
  % If the request is for known topics, then we can safely ignore it, otherwise,
  % queue a metadata request
  NewState = case SortedTopics -- KnownTopics of
               [] -> State;
               _  -> request_metadata([]),
                     State#state{known_topics = lists:umerge(KnownTopics,
                                                             SortedTopics)}
             end,
  {next_state, requesting, NewState};
% Handle the updated metadata
requesting({metadata_updated, RawMapping}, State) ->
  % Create the topic mapping (this also starts the broker connections)
  NewMapping = get_broker_mapping(RawMapping, State),
  _ = lager:debug("Refreshed topic mapping: ~p", [NewMapping]),
  ok = kafkerl_connector:topic_mapping_updated(NewMapping),
  {next_state, idle, State};
% If we have no more retries left, go on cooldown
requesting({metadata_retry, 0}, State = #state{cooldown = Cooldown}) ->
  Params = [?MODULE, on_timer],
  {ok, _} = timer:apply_after(Cooldown, gen_fsm, send_event, Params),
  {next_state, on_cooldown, State};
% If we have more retries to do, schedule a new retry
requesting({metadata_retry, Retries}, State) ->
  ok = schedule_metadata_request(Retries, State),
  {next_state, requesting, State}.

-spec on_cooldown(any(), state()) -> {next_state, atom(), state()}.
on_cooldown({request, NewTopics}, State = #state{known_topics = KnownTopics}) ->
  % Since we are on cooldown (the time between consecutive requests) we only add
  % the topics to the scheduled next request
  SortedTopics = lists:usort(NewTopics),
  State#state{known_topics = lists:umerge(KnownTopics, SortedTopics)};
on_cooldown(on_timer, State) ->
  ok = schedule_metadata_request(State),
  {next_state, requesting, State}.

%%==============================================================================
%% Events
%%==============================================================================
handle_sync_event(get_known_topics, _From, StateName, State) ->
  Reply = State#state.known_topics,
  {reply, Reply, StateName, State}.

%%==============================================================================
%% gen_fsm boilerplate
%%==============================================================================
-spec handle_info(any(), atom(), state()) -> {next_state, atom(), state()}.
handle_info(Message, StateName, State) ->
  lager:info("received unexpected message ~p", [Message]),
  {next_state, StateName, State}.

-spec code_change(any(), atom(), state(), any()) -> {ok, atom(), state()}.
code_change(_OldVsn, StateName, StateData, _Extra) ->
  {ok, StateName, StateData}.

-spec terminate(any(), atom(), state()) -> ok.
terminate(_Reason, _StateName, _StateData) ->
  ok.

-spec handle_event(any(), atom(), state()) -> {next_state, atom(), state()}.
handle_event(_Event, StateName, StateData) ->
  {next_state, StateName, StateData}.

%-spec handle_sync_event(any(), any(), atom(), state()) ->
%  {next_state, atom(), state()}.
%handle_sync_event(_Event, _From, StateName, StateData) ->
%  {next_state, StateName, StateData}.

%%==============================================================================
%% Handlers
%%==============================================================================
init([Config]) ->
  Schema = [{client_id, binary, {default, <<"kafkerl_client">>}},
            {metadata_tcp_timeout, positive_integer, {default, 1500}},
            {metadata_request_cooldown, positive_integer, {default, 333}},
            {max_metadata_retries, {integer, {-1, undefined}}, {default, -1}},
            {brokers, [{string, {integer, {1, 65535}}}], required},
            {topics, [binary], required}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [ClientId, RetryInterval, Cooldown, MaxRetries, Brokers, Topics]} ->
      State = #state{config         = Config,
                     known_topics   = Topics,
                     brokers        = Brokers,
                     cooldown       = Cooldown,
                     client_id      = ClientId,
                     max_retries    = MaxRetries,
                     retry_interval = RetryInterval},
      {ok, idle, State};
    {errors, Errors} ->
      ok = lists:foreach(fun(E) ->
                           _ = lager:critical("Metadata config error ~p", [E])
                         end, Errors),
      {stop, bad_config}
  end.

%%==============================================================================
%% Request logic
%%==============================================================================
schedule_metadata_request(State) ->
  schedule_metadata_request(undefined, State).

schedule_metadata_request(Retries, State = #state{brokers = Brokers,
                                                  max_retries = MaxRetries,
                                                  known_topics = Topics,
                                                  retry_interval = Interval}) ->
  Request = metadata_request(State, Topics),
  case Retries of
    undefined  ->
      Params = [Brokers, Request, MaxRetries],
      _ = spawn(?MODULE, make_request, Params);
    _ ->
      Params = [Brokers, Request, Retries],
      {ok, _} = timer:apply_after(Interval, ?MODULE, make_request, Params)
  end,
  ok.

make_request(Brokers, Request, Retries) ->
  case do_request_metadata(Brokers, Request) of
    {ok, TopicMapping} ->
      gen_fsm:send_event(?MODULE, {metadata_updated, TopicMapping});
    Error ->
      _ = lager:debug("Metadata request error: ~p", [Error]),
      NewRetries = case Retries of -1 -> -1; _ -> Retries - 1 end,
      gen_fsm:send_event(?MODULE, {metadata_retry, NewRetries})
  end.

do_request_metadata([], _Request) ->
  {error, all_down};
do_request_metadata([{Host, Port} = _Broker | T], Request) ->
  _ = lager:debug("Attempting to connect to broker at ~s:~p", [Host, Port]),
  % Connect to the Broker
  case gen_tcp:connect(Host, Port, get_metadata_tcp_options()) of
    {error, Reason} ->
      log_metadata_request_error(Host, Port, Reason),
      % Failed, try with the next one in the list
      do_request_metadata(T, Request);
    {ok, Socket} ->
      % On success, send the metadata request
      case gen_tcp:send(Socket, Request) of
        {error, Reason} ->
          log_metadata_request_error(Host, Port, Reason),
          % Unable to send request, try the next broker
          do_request_metadata(T, Request);
        ok ->
          case gen_tcp:recv(Socket, 0, 6000) of
            {error, Reason} ->
              log_metadata_request_error(Host, Port, Reason),
              gen_tcp:close(Socket),
              % Nothing received (probably a timeout), try the next broker
              do_request_metadata(T, Request);
            {ok, Data} ->
              gen_tcp:close(Socket),
              case kafkerl_protocol:parse_metadata_response(Data) of
                {error, Reason} ->
                  log_metadata_request_error(Host, Port, Reason),
                  % The parsing failed, try the next broker
                  do_request_metadata(T, Request);
                {ok, _CorrelationId, Metadata} ->
                  % We received a metadata response, make sure it has brokers
                  {ok, get_topic_mapping(Metadata)}
              end
          end
      end
  end.

%%==============================================================================
%% Utils
%%==============================================================================
get_metadata_tcp_options() ->
  kafkerl_utils:get_tcp_options([{active, false}, {packet, 4}]).

log_metadata_request_error(Host, Port, Reason) ->
  _ = lager:warning("Unable to retrieve metadata from ~s:~p, reason: ~p",
                    [Host, Port, Reason]).

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
  case kafkerl_broker_connection:start_link(N, Address, Config) of
    {ok, Name, _Pid} ->
      Name;
    {error, {already_started, Pid}} ->
      kafkerl_broker_connection:clear_buffers(Pid),
      Pid
  end.