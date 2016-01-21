-module(kafkerl_broker_connection).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([add_buffer/2, clear_buffers/1, fetch/4, stop_fetch/3]).
% Only for internal use
-export([connect/6]).
% Supervisors
-export([start_link/4]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type conn_idx()            :: 0..1023.
-type start_link_response() :: {ok, atom(), pid()} | ignore | {error, any()}.

-record(fetch, {correlation_id     = 0 :: kafkerl_protocol:correlation_id(),
                server_ref = undefined :: kafkerl:server_ref(),
                topic      = undefined :: kafkerl:topic(),
                partition  = undefined :: kafkerl:partition(),
                options    = undefined :: kafkerl:options(),
                state           = void :: kafkerl_protocol:fetch_state()}).

-record(state, {name      = undefined :: atom(),
                buffers          = [] :: [atom()],
                conn_idx  = undefined :: conn_idx(),
                client_id = undefined :: binary(),
                socket    = undefined :: port(),
                address   = undefined :: kafkerl_connector:address(),
                connector = undefined :: pid(),
                tref      = undefined :: any(),
                tcp_options      = [] :: [any()],
                max_retries       = 0 :: integer(),
                retry_interval    = 0 :: integer(),
                request_number    = 0 :: integer(),
                pending_requests = [] :: [integer()],
                max_time_queued   = 0 :: integer(),
                ets       = undefined :: atom(),
                fetches          = [] :: [#fetch{}],
                current_fetch  = void :: void | kafkerl_protocol:correlation_id()}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(conn_idx(), pid(), kafkerl_connector:address(), any()) ->
  start_link_response().
start_link(Id, Connector, Address, Config) ->
  NameStr = atom_to_list(?MODULE) ++ "_" ++ integer_to_list(Id),
  Name = list_to_atom(NameStr),
  Params = [Id, Connector, Address, Config, Name],
  case gen_server:start_link({local, Name}, ?MODULE, Params, []) of
    {ok, Pid} ->
      {ok, Name, Pid};
    Other ->
      Other
  end.

-spec add_buffer(kafkerl:server_ref(), atom()) -> ok.
add_buffer(ServerRef, Buffer) ->
  gen_server:call(ServerRef, {add_buffer, Buffer}).

-spec clear_buffers(kafkerl:server_ref()) -> ok.
clear_buffers(ServerRef) ->
  gen_server:call(ServerRef, {clear_buffers}).

-spec fetch(kafkerl:server_ref(), kafkerl:topic(), kafkerl:partition(), kafkerl:options()) -> ok.
fetch(ServerRef, Topic, Partition, Options) ->
  gen_server:call(ServerRef, {fetch, ServerRef, Topic, Partition, Options}).

-spec stop_fetch(kafkerl:server_ref(), kafkerl:topic(), kafkerl:partition()) -> ok.
stop_fetch(ServerRef, Topic, Partition) ->
  gen_server:call(ServerRef, {stop_fetch, Topic, Partition}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()}.
handle_call({add_buffer, Buffer}, _From, State = #state{buffers = Buffers}) ->
  {reply, ok, State#state{buffers = [Buffer| Buffers]}};
handle_call({clear_buffers}, _From, State) ->
  {reply, ok, State#state{buffers = []}};
handle_call({fetch, ServerRef, Topic, Partition, Options}, _From, State) ->
  handle_fetch(ServerRef, Topic, Partition, Options, State);
handle_call({stop_fetch, Topic, Partition}, _From, State) ->
  handle_stop_fetch(Topic, Partition, State).

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({connected, Socket}, State) ->
  handle_flush(State#state{socket = Socket});
handle_info(connection_timeout, State) ->
  {stop, {error, unable_to_connect}, State};
handle_info({tcp_closed, _Socket}, State = #state{name = Name,
                                                  address = {Host, Port}}) ->
  _ = lager:warning("~p lost connection to ~p:~p", [Name, Host, Port]),
  NewState = handle_tcp_close(State),
  {noreply, NewState};
handle_info({tcp, _Socket, Bin}, State) ->
  case handle_tcp_data(Bin, State) of
    {ok, NewState}  -> {noreply, NewState};
    {error, Reason} -> {stop, {error, Reason}, State}
  end;
handle_info({flush, Time}, State) ->
  {ok, _Tref} = queue_flush(Time),
  handle_flush(State);
handle_info(Msg, State = #state{name = Name}) ->
  _ = lager:notice("~p received unexpected info message: ~p on ~p", [Name, Msg]),
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
init([Id, Connector, Address, Config, Name]) ->
  Schema = [{tcp_options, [any], {default, []}},
            {retry_interval, positive_integer, {default, 1000}},
            {max_retries, positive_integer, {default, 3}},
            {client_id, binary, {default, <<"kafkerl_client">>}},
            {max_time_queued, positive_integer, required}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [TCPOpts, RetryInterval, MaxRetries, ClientId, MaxTimeQueued]} ->
      NewTCPOpts = kafkerl_utils:get_tcp_options(TCPOpts),
      EtsName = list_to_atom(atom_to_list(Name) ++ "_ets"),
      ets:new(EtsName, [named_table, public, {write_concurrency, true},
                        {read_concurrency, true}]),
      State = #state{conn_idx = Id, tcp_options = NewTCPOpts, address = Address,
                     max_retries = MaxRetries, retry_interval = RetryInterval,
                     connector = Connector, client_id = ClientId, name = Name,
                     max_time_queued = MaxTimeQueued, ets = EtsName},
      Params = [self(), Name, NewTCPOpts, Address, RetryInterval, MaxRetries],
      _Pid = spawn_link(?MODULE, connect, Params),
      {ok, _Tref} = queue_flush(MaxTimeQueued),
      {ok, State};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      _ = lager:critical("broker connection config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_flush(State = #state{socket = undefined}) ->
  {noreply, State};
handle_flush(State = #state{buffers = []}) ->
  {noreply, State};
handle_flush(State = #state{socket = Socket, ets = EtsName, buffers = Buffers,
                            client_id = ClientId, connector = Connector,
                            name = Name}) ->
  {ok, CorrelationId, NewState} = build_correlation_id(State),
  % TODO: Maybe buffer all this messages in case something goes wrong
  AllMessages = get_all_messages(Buffers),
  case kafkerl_utils:merge_messages(AllMessages) of
    [] ->
      {noreply, NewState};
    MergedMessages ->
      Request = kafkerl_protocol:build_produce_request(MergedMessages,
                                                       ClientId,
                                                       CorrelationId),
      true = ets:insert_new(EtsName, {CorrelationId, MergedMessages}),
      _ = lager:debug("~p sending ~p", [Name, Request]),
      case gen_tcp:send(Socket, Request) of
        {error, Reason} ->
          _ = lager:critical("~p was unable to write to socket, reason: ~p",
                             [Name, Reason]),
          gen_tcp:close(Socket),
          ets:delete_all_objects(EtsName, CorrelationId),
          ok = resend_messages(MergedMessages, Connector),
          {noreply, handle_tcp_close(NewState)};
        ok ->
          _ = lager:debug("~p sent message ~p", [Name, CorrelationId]),
          {noreply, NewState}
      end
  end.

handle_fetch(ServerRef, Topic, Partition, Options,
             State = #state{fetches = Fetches, client_id = ClientId,
                            socket = Socket, name = Name}) ->
  {ok, CorrelationId, NewState} = build_correlation_id(State),  
  Offset = proplists:get_value(offset, Options, 0),
  Request = {Topic, {Partition, Offset, 2147483647}},
  MaxWait = proplists:get_value(max_wait, Options),
  MinBytes = proplists:get_value(min_bytes, Options),
  Payload = kafkerl_protocol:build_fetch_request(Request,
                                                 ClientId,
                                                 CorrelationId,
                                                 MaxWait,
                                                 MinBytes),
  case gen_tcp:send(Socket, Payload) of
    {error, Reason} ->
      _ = lager:critical("~p was unable to write to socket, reason: ~p",
                         [Name, Reason]),
      ok = gen_tcp:close(Socket),
      {reply, {error, no_connection}, handle_tcp_close(State)};
    ok ->
      _ = lager:debug("~p sent request ~p", [Name, CorrelationId]),
      NewFetch = #fetch{correlation_id = CorrelationId,
                        server_ref = ServerRef,
                        topic = Topic,
                        partition = Partition,
                        options = Options},
      {reply, ok, NewState#state{fetches = [NewFetch | Fetches]}}
  end;
handle_fetch(_ServerRef, _Topic, _Partition, _Options, State) ->
  {reply, {error, fetch_in_progress}, State}.

handle_stop_fetch(Topic, Partition, State) ->
  % Leave current fetch as it is
  NewFetches = remove_fetch(Topic, Partition, State#state.fetches),
  {reply, ok, State#state{fetches = NewFetches}}.

remove_fetch(Topic, Partition, CurrentFetches) ->
  remove_fetch(Topic, Partition, CurrentFetches, []).
remove_fetch(_Topic, _Partition, [], Acc) ->
  Acc;
remove_fetch(Topic, Partition,
             [#fetch{topic = Topic, partition = Partition} = Fetch | T], Acc) ->
  % Clearing the fetch options ensures this fetch will stop sending any messages
  % since there is no consumer. This also removes the fetch_interval so it won't
  % be requested again.
  % Simply removing the fetch here doesn't work since we will still get a server
  % response, but we will not be able to properly handle it.
  [Fetch#fetch{options = []} | Acc] ++ T;
remove_fetch(Topic, Partition, [H | T], Acc) ->
  remove_fetch(Topic, Partition, T, [H | Acc]).

% TCP Handlers
handle_tcp_close(State = #state{retry_interval = RetryInterval,
                                tcp_options = TCPOpts,
                                max_retries = MaxRetries,
                                address = Address,
                                name = Name}) ->
  Params = [self(), Name, TCPOpts, Address, RetryInterval, MaxRetries],
  _Pid = spawn_link(?MODULE, connect, Params),
  State#state{socket = undefined}.

handle_tcp_data(Bin, State = #state{fetches = Fetches,
                                    current_fetch = CurrentFetch}) ->
  {ok, CorrelationId, _NewBin} = parse_correlation_id(Bin, CurrentFetch),
  case get_fetch(CorrelationId, Fetches) of
    Fetch = #fetch{} ->
      handle_fetch_response(Bin, Fetch, State);
    _ ->
      handle_produce_response(Bin, State)
  end.

handle_fetch_response(Bin, Fetch, State = #state{name = Name,
                                                 fetches = Fetches}) ->
  Options = Fetch#fetch.options,
  Consumer = proplists:get_value(consumer, Options),
  case kafkerl_protocol:parse_fetch_response(Bin, Fetch#fetch.state) of
    {ok, _CorrelationId, [{_, [{{_, Offset}, Messages}]}]} ->
      % The messages can be empty, for example when there are no new messages in
      % this partition, if that happens, don't send anything and end the fetch.
      ok = send_messages(Consumer,
                         case Messages of
                           [] -> [];
                           _  -> [{consumed, Messages}, {offset, Offset}]
                         end),
      case proplists:get_value(fetch_interval, Options, false) of
        false    -> {ok, State#state{current_fetch = void}};
        Interval -> 
                    NewOptions = lists:keyreplace(offset, 1, Options, {offset, Offset}),
                    Arguments = [Fetch#fetch.server_ref, Fetch#fetch.topic,
                                 Fetch#fetch.partition, NewOptions],
                    _ = timer:apply_after(Interval, ?MODULE, fetch, Arguments),
                    {ok, State#state{current_fetch = void,
                                     fetches = lists:delete(Fetch, Fetches)}}
      end;
    {incomplete, CorrelationId, Data, NewFetchState} ->
      ok = case Data of
             [{_, [{_, Messages}]}] ->
               send_messages(Consumer, {consumed, Messages});
             _ ->
               % On some cases, kafka will return an incomplete response with no
               % messages, in this case since we don't have anything to send, we
               % just need to update the fetch state.
               ok
           end,
      {ok, State#state{fetches = [Fetch#fetch{state = NewFetchState} |
                                  lists:delete(Fetch, Fetches)],
                       current_fetch = CorrelationId}};
    Error ->
      ok = send_messages(Consumer, Error),
      {ok, State#state{current_fetch = void, fetches = lists:delete(Fetch, Fetches)}}
  end.

handle_produce_response(Bin, State = #state{connector = Connector, name = Name,
                                            ets = EtsName}) ->
  case kafkerl_protocol:parse_produce_response(Bin) of
    {ok, CorrelationId, Topics} ->
      case ets:lookup(EtsName, CorrelationId) of
        [{CorrelationId, Messages}] ->
          ets:delete(EtsName, CorrelationId),
          {Errors, Successes} = split_errors_and_successes(Topics),
          % First, send the offsets and messages that were delivered
          spawn(fun() ->
                  notify_success_to_connector(Successes, Messages, Connector)
                end),
          case handle_errors(Errors, Messages, Name) of
            ignore ->
              {ok, State};
            {request_metadata, MessagesToResend} ->
              kafkerl_connector:request_metadata(Connector),
              ok = resend_messages(MessagesToResend, Connector),
              {ok, State}
          end;
        _ ->
          _ = lager:warning("~p was unable to properly process produce response",
                            [Name]),
          {error, invalid_produce_response}
      end;
    Other ->
     _ = lager:critical("~p got unexpected response when parsing message: ~p",
                        [Name, Other]),
     {ok, State}
  end.

%%==============================================================================
%% Utils
%%==============================================================================
resend_messages(Messages, Connector) ->
  F = fun(M) -> kafkerl_connector:send(Connector, M, []) end,
  lists:foreach(F, Messages).

notify_success_to_connector([], _Messages, _Pid) ->
  ok;
notify_success_to_connector([{Topic, Partition, Offset} | T], Messages, Pid) ->
  MergedMessages = kafkerl_utils:merge_messages(Messages),
  Partitions = partitions_in_topic(Topic, MergedMessages),
  M = messages_in_partition(Partition, Partitions),
  kafkerl_connector:produce_succeeded(Pid, {Topic, Partition, M, Offset}),
  notify_success_to_connector(T, Messages, Pid).
  
partitions_in_topic(Topic, Messages) ->
  lists:flatten([P || {T, P} <- Messages, T =:= Topic]).
messages_in_partition(Partition, Messages) ->
  lists:flatten([M || {P, M} <- Messages, P =:= Partition]).

build_correlation_id(State = #state{request_number = RequestNumber,
                                    conn_idx = ConnIdx}) ->
  % CorrelationIds are 32 bit integers, of those, the first 10 bits are used for
  % the connectionId (hence the 1023 limit on it) and the other 22 bits are used
  % for the sequential numbering, this magic number down here is actually 2^10-1
  NextRequest = case RequestNumber > 4194303 of
                  true -> 0;
                  false -> RequestNumber + 1
                end,
  CorrelationId = (ConnIdx bsl 22) bor NextRequest,
  {ok, CorrelationId, State#state{request_number = NextRequest}}.

split_errors_and_successes(Topics) ->
  split_errors_and_successes(Topics, {[], []}).

split_errors_and_successes([], Acc) ->
  Acc;
split_errors_and_successes([{Topic, Partitions} | T], Acc) ->
  F = fun({Partition, ?NO_ERROR, Offset}, {E, S}) ->
           {E, [{Topic, Partition, Offset} | S]};
         ({Partition, Error, _}, {E, S}) ->
           {[{Topic, Partition, Error} | E], S}
      end,
  split_errors_and_successes(T, lists:foldl(F, Acc, Partitions)).

handle_errors([], _Messages, _Name) ->
  ignore;
handle_errors(Errors, Messages, Name) ->
  F = fun(E) -> handle_error(E, Messages, Name) end,
  case lists:filtermap(F, Errors) of
    [] -> ignore;
    L  -> {request_metadata, L}
  end.

handle_error({Topic, Partition, Error}, Messages, Name)
  when Error =:= ?UNKNOWN_TOPIC_OR_PARTITION orelse
       Error =:= ?NOT_LEADER_FOR_PARTITION orelse
       Error =:= ?LEADER_NOT_AVAILABLE ->
  case get_message_for_error(Topic, Partition, Messages, Name) of
    undefined -> false;
    Message   -> {true, Message}
  end;
handle_error({Topic, Partition, Error}, _Messages, Name) ->
  _ = lager:error("~p was unable to handle ~p error on topic ~p, partition ~p",
                  [Name, kafkerl_error:get_error_name(Error), Topic, Partition]),
  false.

get_message_for_error(Topic, Partition, SavedMessages, Name) ->
  case lists:keyfind(Topic, 1, SavedMessages) of
    false ->
      _ = lager:error("~p found no messages for topic ~p, partition ~p",
                      [Name, Topic, Partition]),
      undefined;
    {Topic, Partitions} ->
      case lists:keyfind(Partition, 1, Partitions) of
        false -> 
          _ = lager:error("~p found no messages for topic ~p, partition ~p",
                          [Name, Topic, Partition]),
          undefined;
        {Partition, Messages} ->
          {Topic, Partition, Messages}
      end
  end.

connect(Pid, Name, _TCPOpts, {Host, Port} = _Address, _Timeout, 0) ->
  _ = lager:error("~p was unable to connect to ~p:~p", [Name, Host, Port]),
  Pid ! connection_timeout;
connect(Pid, Name, TCPOpts, {Host, Port} = Address, Timeout, Retries) ->
  _ = lager:debug("~p attempting connection to ~p:~p", [Name, Host, Port]),
  case gen_tcp:connect(Host, Port, TCPOpts, 5000) of
    {ok, Socket} ->
      _ = lager:debug("~p connnected to ~p:~p", [Name, Host, Port]),
      gen_tcp:controlling_process(Socket, Pid),
      Pid ! {connected, Socket};
    {error, Reason} ->
      NewRetries = Retries - 1,
      _ = lager:warning("~p can't connect to ~p:~p. Reason: ~p, ~p retries left",
                        [Name, Host, Port, Reason, NewRetries]),
      timer:sleep(Timeout),
      connect(Pid, Name, TCPOpts, Address, Timeout, NewRetries)
  end.

queue_flush(Time) ->
  timer:send_after(Time * 1000, {flush, Time}).

get_all_messages(Buffers) ->
  get_all_messages(Buffers, []).

get_all_messages([], Acc) ->
  Acc;
get_all_messages([H | T], Acc) ->
  get_all_messages(T, Acc ++ get_messages_from(H, 20)).

get_messages_from(Ets, Retries) ->
  case ets_buffer:read_all(Ets) of
    L when is_list(L) ->
      L;
    _Error when Retries > 0 ->
      get_messages_from(Ets, Retries - 1);
    _Error ->
      _ = lager:warning("giving up on reading from the ETS buffer"),
      []
  end.

parse_correlation_id(Bin, void) ->
  kafkerl_protocol:parse_correlation_id(Bin);
parse_correlation_id(Bin, CorrelationId) ->
  {ok, CorrelationId, Bin}.

get_fetch(_CorrelationId, []) ->
  not_found;
get_fetch(CorrelationId, [Fetch = #fetch{correlation_id = CorrelationId} | _T]) ->
  Fetch;
get_fetch(CorrelationId, [_ | T]) ->
  get_fetch(CorrelationId, T).

send_messages(_Consumer, []) ->
  ok;
send_messages(Consumer, [Event | T]) ->
  case send_messages(Consumer, Event) of
    ok     -> send_messages(Consumer, T);
    Error  -> Error
  end;
send_messages(Consumer, Event) ->
  kafkerl_utils:send_event(Consumer, Event).