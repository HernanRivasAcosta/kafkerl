-module(kafkerl_broker_connection).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
-export([send/2, flush/1]).
% Only for internal use
-export([connect/5]).
% Supervisors
-export([start_link/4]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type server_ref()          :: atom() | pid().
-type conn_idx()           :: 0..1023.
-type start_link_response() :: {ok, atom(), pid()} | ignore | {error, any()}.

-record(state, {conn_idx  = undefined :: conn_idx(),
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
  Params = [Id, Connector, Address, Config],
  Name = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ integer_to_list(Id)),
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

% gen_server callbacks
-spec handle_call(any(), any(), state()) -> {reply, ok, state()}.
handle_call({send, Message}, _From, State) ->
  handle_send(Message, State);
handle_call({flush}, _From, State) ->
  handle_flush(State).

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({connected, Socket}, State) ->
  {reply, ok, NewState} = handle_flush(State#state{socket = Socket}),
  {noreply, NewState};
handle_info(connection_timeout, State) ->
  {stop, {error, unable_to_connect}, State};
handle_info({tcp_closed, _Socket}, State) ->
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
init([Id, Connector, Address, Config]) ->
  Schema = [{tcp_options, [any], {default, []}},
            {retry_interval, positive_integer, {default, 1000}},
            {max_retries, positive_integer, {default, 3}},
            {client_id, binary, {default, <<"kafkerl_client">>}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [TCPOpts, RetryInterval, MaxRetries, ClientId]} ->
      NewTCPOpts = get_tcp_options(TCPOpts),
      State = #state{conn_idx = Id, address = Address, connector = Connector,
                     max_retries = MaxRetries, retry_interval = RetryInterval,
                     tcp_options = NewTCPOpts, client_id = ClientId},
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
  ok = case BuiltRequest of
         {ok, void} ->
           ok;
         {ok, IOList} ->
           case gen_tcp:send(Socket, IOList) of
             {error, Reason} ->
               lager:warning("unable to write to socket, reason: ~p", [Reason]),
               gen_tcp:close(Socket),
               reconnect;
             ok ->
               ok
           end
       end,
  {reply, ok, NewState}.

% TCP Handlers
handle_tcp_close(State = #state{retry_interval = RetryInterval,
                                tcp_options = TCPOpts,
                                max_retries = MaxRetries,
                                address = Address}) ->
  lager:warning("lost connection to ~p:~p"),
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
          Errors = get_errors_from_produce_response(Topics),
          case handle_errors(Errors, Messages) of
            {ignore, _} ->
              State;
            {request_metadata, MessagesToResend} ->
              Connector ! force_metadata_request,
              F = fun(M) -> kafkerl_connector:send(Connector, M) end,
              ok = lists:foreach(F, MessagesToResend),
              State
          end,
          State
      end;
    Other ->
     lager:critical("unexpected response when parsing message: ~p", [Other]),
     State
  end.

%%==============================================================================
%% Utils
%%==============================================================================
get_tcp_options(Options) -> % TODO: refactor
  lists:ukeymerge(1, lists:sort(proplists:unfold(Options)), ?DEFAULT_TCP_OPTS).

build_correlation_id(State = #state{request_number = RequestNumber,
                                    conn_idx = ConnIdx}) ->
  % CorrelationIds are 32 bit integers, of those, the first 10 bits are used for
  % the connectionId (hence the 1023 limit on it) and the other 22 are for the
  % sequential, this magic number down here is simply 2^10-1
  NextRequest = case RequestNumber > 4194303 of
                  true -> 0;
                  false -> RequestNumber + 1
                end,
  lager:notice("ConnIdx: ~p", [ConnIdx]),
  lager:notice("(ConnIdx bsl 22): ~p", [(ConnIdx bsl 22)]),
  lager:notice("NextRequest: ~p", [NextRequest]),
  CorrelationId = (ConnIdx bsl 22) bor NextRequest,
  {ok, CorrelationId, State#state{request_number = NextRequest}}.

get_errors_from_produce_response(Topics) ->
  get_errors_from_produce_response(Topics, []).

get_errors_from_produce_response([{Topic, Partitions} | T], Acc) ->
  Errors = [{Topic, Partition, Error} ||
            {Partition, Error, _} <- Partitions, Error =/= ?NO_ERROR],
  get_errors_from_produce_response(T, Acc ++ Errors).

handle_errors([] = Errors, _Messages) ->
  {ignore, Errors};
handle_errors(Errors, Messages) ->
  F = fun(E, Acc) -> handle_error(E, Messages, Acc) end,
  lists:foreach(F, {ignore, []}, Errors).

handle_error({Topic, Partition, Error}, Messages, {_Status, Acc} = Status)
  when Error =:= ?UNKNOWN_TOPIC_OR_PARTITION orelse
       Error =:= ?LEADER_NOT_AVAILABLE orelse
       Error =:= ?NOT_LEADER_FOR_PARTITION ->
  case get_message_for_error(Topic, Partition, Messages) of
    undefined -> Status;
    Message   -> {request_metadata, [Message | Acc]}
  end;
handle_error({Topic, Partition, Error}, _Messages, Acc) ->
  lager:error("Unable to handle ~p error on topic ~p, partition ~p",
              [kafkerl_error:get_error_name(Error), Topic, Partition]),
  Acc.

get_message_for_error(Topic, Partition, []) ->
  lager:error("no saved message found for error on topic ~p, partition ~p",
              [Topic, Partition]),
  undefined;
get_message_for_error(Topic, Partition, [{Topic, Partition, _} = H | _T]) ->
  H;
get_message_for_error(Topic, Partition, [_Message | T]) ->
  get_message_for_error(Topic, Partition, T).

connect(Pid, _TCPOpts, {Host, Port} = _Address, _Timeout, 0) ->
  lager:error("unable to connect to ~p:~p", [Host, Port]),
  Pid ! connection_timeout;
connect(Pid, TCPOpts, {Host, Port} = Address, Timeout, Retries) ->
  lager:debug("Attempting connection to broker at ~p:~p", [Host, Port]),
  case gen_tcp:connect(binary_to_list(Host), Port, TCPOpts, 1000) of
    {ok, Socket} ->
      Pid ! {connected, Socket};
    {error, Reason} ->
      NewRetries = Retries - 1,
      lager:warning("Unable to connect to ~p:~p. Reason:~p, ~p retries left",
                    [Host, Port, Reason, NewRetries]),
      timer:sleep(Timeout),
      connect(Pid, TCPOpts, Address, Timeout, NewRetries)
  end.