-module(kafkerl_metadata_requester).
-author("martin").

-behaviour(gen_server).

%% API
-export([start_link/0, req_metadata/5]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

-define(SERVER, ?MODULE).

-record(state, {
  last_metadata_req_time :: undefined | erlang:timestamp()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

req_metadata(Brokers, TCPOpts, Retries, RetryInterval, Request) ->
  gen_server:call(?MODULE, {do_req_metadata, Brokers, TCPOpts, Retries, RetryInterval, Request}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #state{}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({do_req_metadata, Brokers, TCPOpts, Retries, RetryInterval, Request}, _From, State) ->
  Response = do_request_metadata(Brokers, TCPOpts, Retries, RetryInterval, Request),
  {reply, Response, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_request_metadata(_Brokers, _TCPOpts, 0, _RetryInterval, _Request) ->
  metadata_timeout;
do_request_metadata(Brokers, TCPOpts, Retries, RetryInterval, Request) ->
  case do_request_metadata(Brokers, TCPOpts, Request) of
    {ok, TopicMapping} ->
      {metadata_updated, TopicMapping};
    _Error ->
      timer:sleep(RetryInterval),
      NewRetries = case Retries of
                     -1 -> -1;
                     N  -> N - 1
                   end,
      req_metadata(Brokers, TCPOpts, NewRetries, RetryInterval, Request)
  end.

do_request_metadata([], _TCPOpts, _Request) ->
  {error, all_down};
do_request_metadata([{Host, Port} = _Broker | T] = _Brokers, TCPOpts, Request) ->
  lager:debug("Attempting to connect to broker at ~s:~p", [Host, Port]),
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
  lager:warning("Ignoring ~p on metadata for topic ~p",
    [kafkerl_error:get_error_name(Error), Topic]),
  {true, {Topic, Partitions}};
expand_topic({Error, Topic, _Partitions}) ->
  lager:error("Error ~p on metadata for topic ~p",
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
  lager:warning("Ignoring ~p on metadata for topic ~p, partition ~p",
    [kafkerl_error:get_error_name(Error), Topic, Partition]),
  ExpandedPartition = {{Topic, Partition}, Leader},
  expand_partitions({Topic, T}, [ExpandedPartition | Acc]);
expand_partitions({Topic, [{Error, Partition, _, _, _} | T]}, Acc) ->
  lager:error("Error ~p on metadata for topic ~p, partition ~p",
    [kafkerl_error:get_error_name(Error), Topic, Partition]),
  expand_partitions({Topic, T}, Acc).



%%==============================================================================
%% Error handling
%%==============================================================================
warn_metadata_request(Host, Port, Reason) ->
  lager:warning("Unable to retrieve metadata from ~s:~p, reason: ~p",
    [Host, Port, Reason]).
