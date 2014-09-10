-module(kafkerl_broker_connection).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([send/2, flush/1, kill/1]).
% Only for internal use
-export([connect/5]).
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
                pending_requests = [] :: [integer()]}).
-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(conn_idx(), pid(), socket_address(), any()) ->
  start_link_response().
start_link(Id, Connector, Address, Config) ->
  Name = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(Id)),
  Params = [Id, Connector, Address, Config, Name],
  case gen_server:start_link({local, Name}, ?MODULE, Params, []) of
    {ok, Pid} ->
      {ok, Name, Pid};
    Other ->
      Other
  end.

-spec send(server_ref(), basic_message()) -> ok.
send(ServerRef, Message) ->
  gen_server:call(ServerRef, {send, Message}).

-spec flush(server_ref()) -> ok.
flush(ServerRef) ->
  gen_server:call(ServerRef, {flush}).

-spec kill(server_ref()) -> ok.
kill(ServerRef) ->
  gen_server:call(ServerRef, {kill}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()}.
handle_call({send, Message}, _From, State) ->
  handle_send(Message, State);
handle_call({flush}, _From, State) ->
  handle_flush(State);
handle_call({kill}, _From, State = #state{name = Name}) ->
  % TODO: handle the potentially buffered messages
  lager:info("~p stopped by it's parent connector", [Name]),
  {stop, normal, ok, State}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({connected, Socket}, State) ->
  {reply, ok, NewState} = handle_flush(State#state{socket = Socket}),
  {noreply, NewState};
handle_info(connection_timeout, State) ->
  {stop, {error, unable_to_connect}, State};
handle_info({tcp_closed, _Socket}, State = #state{address = {Host, Port}}) ->
  lager:warning("lost connection to ~p:~p", [Host, Port]),
  NewState = handle_tcp_close(State),
  {noreply, NewState};
handle_info({tcp, _Socket, Bin}, State) ->
  NewState = handle_tcp_data(Bin, State),
  {noreply, NewState};
handle_info(Msg, State) ->
  lager:notice("unexpected info message received: ~p on ~p", [Msg, State]),
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
            {client_id, binary, {default, <<"kafkerl_client">>}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [TCPOpts, RetryInterval, MaxRetries, ClientId]} ->
      NewTCPOpts = kafkerl_utils:get_tcp_options(TCPOpts),
      State = #state{conn_idx = Id, tcp_options = NewTCPOpts, address = Address,
                     max_retries = MaxRetries, retry_interval = RetryInterval,
                     connector = Connector, client_id = ClientId, name = Name},
      Params = [self(), NewTCPOpts, Address, RetryInterval, MaxRetries],
      _Pid = spawn_link(?MODULE, connect, Params),
      {ok, State};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("broker connection config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_send(Message, State = #state{conn_idx = ConnIdx}) ->
  case kafkerl_buffer:buffer(ConnIdx, Message) of
    {ok, true = _ShouldFlush} ->
      handle_flush(State);
    {ok, false = _ShouldFlush} ->
      {reply, ok, State}
  end.

handle_flush(State = #state{socket = undefined}) ->
  {reply, ok, State};
handle_flush(State = #state{socket = Socket, conn_idx = ConnIdx,
                            client_id = ClientId}) ->
  {ok, CorrelationId, NewState} = build_correlation_id(State),
  BuiltRequest = kafkerl_buffer:build_request(ConnIdx, ClientId, CorrelationId,
                                              ?COMPRESSION_NONE),
  case BuiltRequest of
    {ok, void} ->
      {reply, ok, NewState};
    {ok, IOList} ->
      lager:debug("Sending ~p", [IOList]),
      case gen_tcp:send(Socket, IOList) of
        {error, Reason} ->
          lager:warning("unable to write to socket, reason: ~p", [Reason]),
          gen_tcp:close(Socket),
          {reply, ok, handle_tcp_close(NewState)};
        ok ->
          lager:debug("message ~p sent", [CorrelationId]),
          {reply, ok, NewState}
      end
  end.

% TCP Handlers
handle_tcp_close(State = #state{retry_interval = RetryInterval,
                                tcp_options = TCPOpts,
                                max_retries = MaxRetries,
                                address = Address}) ->
  Params = [self(), TCPOpts, Address, RetryInterval, MaxRetries],
  _Pid = spawn_link(?MODULE, connect, Params),
  State#state{socket = undefined}.

handle_tcp_data(Bin, State = #state{connector = Connector}) ->
  case kafkerl_protocol:parse_produce_response(Bin) of
    {ok, CorrelationId, Topics} ->
      case kafkerl_buffer:delete_saved_request(CorrelationId) of
        {error, Reason} ->
          lager:error("Unable to retrieve the saved request #~p, reason: ~p",
                      [CorrelationId, Reason]),
          State;
        {ok, Messages} ->
          {Errors, Successes} = separate_errors(Topics),
          % First, send the offsets and messages that were delivered
          spawn(fun() ->
                  notify_success_to_connector(Successes, Messages, Connector)
                end),
          case handle_errors(Errors, Messages) of
            ignore ->
              State;
            {request_metadata, MessagesToResend} ->
              kafkerl_connector:request_metadata(Connector),
              F = fun(M) -> kafkerl_connector:send(Connector, M) end,
              ok = lists:foreach(F, MessagesToResend),
              State
          end
      end;
    Other ->
     lager:critical("unexpected response when parsing message: ~p", [Other]),
     State
  end.

%%==============================================================================
%% Utils
%%==============================================================================
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

handle_errors([], _Messages) ->
  ignore;
handle_errors(Errors, Messages) ->
  F = fun(E) -> handle_error(E, Messages) end,
  case lists:filtermap(F, Errors) of
    [] -> ignore;
    L  -> {request_metadata, L}
  end.

handle_error({Topic, Partition, Error}, Messages)
  when Error =:= ?UNKNOWN_TOPIC_OR_PARTITION orelse
       Error =:= ?LEADER_NOT_AVAILABLE orelse
       Error =:= ?NOT_LEADER_FOR_PARTITION ->
  case get_message_for_error(Topic, Partition, Messages) of
    undefined -> false;
    Message   -> {true, Message}
  end;
handle_error({Topic, Partition, Error}, _Messages) ->
  lager:error("Unable to handle ~p error on topic ~p, partition ~p",
              [kafkerl_error:get_error_name(Error), Topic, Partition]),
  false.

get_message_for_error(Topic, Partition, SavedMessages) ->
  case lists:keyfind(Topic, 1, SavedMessages) of
    false ->
      lager:error("No saved messages found for topic ~p, partition ~p",
                  [Topic, Partition]),
      undefined;
    {Topic, Partitions} ->
      case lists:keyfind(Partition, 1, Partitions) of
        false -> 
          lager:error("No saved messages found for topic ~p, partition ~p",
                      [Topic, Partition]),
          undefined;
        {Partition, Messages} ->
          {Topic, Partition, Messages}
      end
  end.

connect(Pid, _TCPOpts, {Host, Port} = _Address, _Timeout, 0) ->
  lager:error("Unable to connect to ~p:~p", [Host, Port]),
  Pid ! connection_timeout;
connect(Pid, TCPOpts, {Host, Port} = Address, Timeout, Retries) ->
  lager:debug("Attempting connection to broker at ~p:~p", [Host, Port]),
  case gen_tcp:connect(Host, Port, TCPOpts, 5000) of
    {ok, Socket} ->
      lager:debug("Connnected to ~p:~p", [Host, Port]),
      gen_tcp:controlling_process(Socket, Pid),
      Pid ! {connected, Socket};
    {error, Reason} ->
      NewRetries = Retries - 1,
      lager:warning("Unable to connect to ~p:~p. Reason:~p, ~p retries left",
                    [Host, Port, Reason, NewRetries]),
      timer:sleep(Timeout),
      connect(Pid, TCPOpts, Address, Timeout, NewRetries)
  end.