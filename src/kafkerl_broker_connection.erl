-module(kafkerl_broker_connection).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([add_buffer/2, clear_buffers/1]).
% Only for internal use
-export([connect/6]).
% Supervisors
-export([start_link/4]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type server_ref()          :: atom() | pid().
-type conn_idx()            :: 0..1023.
-type start_link_response() :: {ok, atom(), pid()} | ignore | {error, any()}.

-record(state, {name      = undefined :: atom(),
                buffers          = [] :: [atom()],
                conn_idx  = undefined :: conn_idx(),
                client_id = undefined :: binary(),
                socket    = undefined :: undefined | port(),
                address   = undefined :: undefined | socket_address(),
                connector = undefined :: undefined | pid(),
                tref      = undefined :: undefined | any(),
                tcp_options      = [] :: [any()],
                max_retries       = 0 :: integer(),
                retry_interval    = 0 :: integer(),
                request_number    = 0 :: integer(),
                pending_requests = [] :: [integer()],
                max_time_queued   = 0 :: integer(),
                ets       = undefined :: atom()}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(conn_idx(), pid(), socket_address(), any()) ->
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

-spec add_buffer(server_ref(), atom()) -> ok.
add_buffer(ServerRef, Buffer) ->
  gen_server:call(ServerRef, {add_buffer, Buffer}).

-spec clear_buffers(server_ref()) -> ok.
clear_buffers(ServerRef) ->
  gen_server:call(ServerRef, {clear_buffers}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()}.
handle_call({add_buffer, Buffer}, _From, State = #state{buffers = Buffers}) ->
  {reply, ok, State#state{buffers = [Buffer| Buffers]}};
handle_call({clear_buffers}, _From, State) ->
  {reply, ok, State#state{buffers = []}}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({connected, Socket}, State) ->
  handle_flush(State#state{socket = Socket});
handle_info(connection_timeout, State) ->
  {stop, {error, unable_to_connect}, State};
handle_info({tcp_closed, _Socket}, State = #state{name = Name,
                                                  address = {Host, Port}}) ->
  lager:warning("~p lost connection to ~p:~p", [Name, Host, Port]),
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
  lager:notice("~p received unexpected info message: ~p on ~p", [Name, Msg]),
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
                      lager:critical("broker connection config error ~p", [E])
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
                                                       CorrelationId,
                                                       ?COMPRESSION_NONE),
      true = ets:insert_new(EtsName, {CorrelationId, MergedMessages}),
      lager:debug("~p sending ~p", [Name, Request]),
      case gen_tcp:send(Socket, Request) of
        {error, Reason} ->
          lager:critical("~p was unable to write to socket, reason: ~p",
                         [Name, Reason]),
          gen_tcp:close(Socket),
          ets:delete_all_objects(EtsName, CorrelationId),
          ok = resend_messages(MergedMessages, Connector),
          {noreply, handle_tcp_close(NewState)};
        ok ->
          lager:debug("~p sent message ~p", [Name, CorrelationId]),
          {noreply, NewState}
      end
  end.

% TCP Handlers
handle_tcp_close(State = #state{retry_interval = RetryInterval,
                                tcp_options = TCPOpts,
                                max_retries = MaxRetries,
                                address = Address,
                                name = Name}) ->
  Params = [self(), Name, TCPOpts, Address, RetryInterval, MaxRetries],
  _Pid = spawn_link(?MODULE, connect, Params),
  State#state{socket = undefined}.

handle_tcp_data(Bin, State = #state{connector = Connector, ets = EtsName,
                                    name = Name}) ->
  case kafkerl_protocol:parse_produce_response(Bin) of
    {ok, CorrelationId, Topics} ->
      case ets:lookup(EtsName, CorrelationId) of
        [{CorrelationId, Messages}] ->
          ets:delete(EtsName, CorrelationId),
          {Errors, Successes} = separate_errors(Topics),
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
          lager:warning("~p was unable to properly process produce response",
                        [Name]),
          {error, invalid_produce_response}
      end;
    Other ->
     lager:critical("~p got unexpected response when parsing message: ~p",
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

% TODO: Refactor this function, it is not sufficiently clear what it does
separate_errors(Topics) ->
  separate_errors(Topics, {[], []}).

separate_errors([], Acc) ->
  Acc;
separate_errors([{Topic, Partitions} | T], Acc) ->
  F = fun({Partition, ?NO_ERROR, Offset}, {E, S}) ->
           {E, [{Topic, Partition, Offset} | S]};
         ({Partition, Error, _}, {E, S}) ->
           {[{Topic, Partition, Error} | E], S}
      end,
  separate_errors(T, lists:foldl(F, Acc, Partitions)).

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
       Error =:= ?LEADER_NOT_AVAILABLE orelse
       Error =:= ?NOT_LEADER_FOR_PARTITION ->
  case get_message_for_error(Topic, Partition, Messages, Name) of
    undefined -> false;
    Message   -> {true, Message}
  end;
handle_error({Topic, Partition, Error}, _Messages, Name) ->
  lager:error("~p was unable to handle ~p error on topic ~p, partition ~p",
              [Name, kafkerl_error:get_error_name(Error), Topic, Partition]),
  false.

get_message_for_error(Topic, Partition, SavedMessages, Name) ->
  case lists:keyfind(Topic, 1, SavedMessages) of
    false ->
      lager:error("~p found no saved messages for topic ~p, partition ~p",
                  [Name, Topic, Partition]),
      undefined;
    {Topic, Partitions} ->
      case lists:keyfind(Partition, 1, Partitions) of
        false -> 
          lager:error("~p found no saved messages for topic ~p, partition ~p",
                      [Name, Topic, Partition]),
          undefined;
        {Partition, Messages} ->
          {Topic, Partition, Messages}
      end
  end.

connect(Pid, Name, _TCPOpts, {Host, Port} = _Address, _Timeout, 0) ->
  lager:error("~p was unable to connect to ~p:~p", [Name, Host, Port]),
  Pid ! connection_timeout;
connect(Pid, Name, TCPOpts, {Host, Port} = Address, Timeout, Retries) ->
  lager:debug("~p attempting connection to ~p:~p", [Name, Host, Port]),
  case gen_tcp:connect(Host, Port, TCPOpts, 5000) of
    {ok, Socket} ->
      lager:debug("~p connnected to ~p:~p", [Name, Host, Port]),
      gen_tcp:controlling_process(Socket, Pid),
      Pid ! {connected, Socket};
    {error, Reason} ->
      NewRetries = Retries - 1,
      lager:warning("~p can't connect to ~p:~p. Reason: ~p, ~p retries left",
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
      lager:warning("giving up on reading from the ETS buffer"),
      []
  end.