-module(kafkerl_consumer).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

-export([request_topics/2, request_topics/3]).

-export([start_link/1, start_link/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type callback()        :: {atom(), atom()} | {atom(), atom(), [any()]} | pid().
-type pending_request() :: {kafkerl_fetch_request(), callback()}.

-record(state, {connector_name = undefined :: atom() | undefined,
                client_id      = undefined :: binary(),
                max_wait               = 0 :: integer(),
                min_bytes              = 1 :: integer(),
                correlation_id         = 0 :: integer(),
                parsing_state       = void :: kafkerl_state() | void | pending,
                pending_requests      = [] :: [pending_request()],
                callback       = undefined :: callback()}).

-type state() :: #state{}.
-type start_link_response() :: {ok, pid()} | ignore | {error, any()}.
-type valid_cast_message() :: {request_topics, kafkerl_fetch_request(),
                               callback()}.

%%==============================================================================
%% API
%%==============================================================================
% Starting the server
-spec start_link(any()) -> start_link_response().
start_link(Options) ->
  start_link(?MODULE, Options).
-spec start_link(atom(), any()) -> start_link_response().
start_link(Name, Options) when is_atom(Name) ->
  gen_server:start_link({local, Name}, ?MODULE, [Options], []).

% Requesting topics
-spec request_topics(kafkerl_fetch_request(), callback()) -> ok.
request_topics(Topics, Callback) ->
  request_topics(?MODULE, Topics, Callback).
-spec request_topics(atom(), kafkerl_fetch_request(), callback()) -> ok.
request_topics(Name, Topics, Callback) ->
  gen_server:cast(Name, {request_topics, Topics, Callback}).

% gen_server callbacks
-spec handle_cast(valid_cast_message(), state()) -> {noreply, state()}.
handle_cast({request_topics, Topic, Callback}, State) when is_tuple(Topic) ->
  handle_cast({request_topics, [Topic], Callback}, State);  
handle_cast({request_topics, Topics, Callback},
            State = #state{correlation_id = CorrelationId,
                           parsing_state  = void}) ->
  FlatTopics = lists:flatten(Topics),
  case handle_request_topics(FlatTopics, State) of
    ok ->
      NewState = State#state{correlation_id = CorrelationId + 1,
                             callback = Callback,
                             parsing_state = pending},
      {noreply, NewState};
    Error ->
      lager:error("an error occurred when requesting topics ~p", [Error]),
      {noreply, State}
  end;
handle_cast({request_topics, Topics, Callback},
            State = #state{pending_requests = PendingRequests}) ->
  % FIFO, probably going to use a better queue implementation in the future
  NewPendingRequests = PendingRequests ++ [{Topics, Callback}],
  {noreply, State#state{pending_requests = NewPendingRequests}}.

-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({kafka_message, Bin},
            State = #state{callback = Callback,
                           parsing_state = ParsingState,
                           pending_requests = PendingRequests}) ->
  case on_binary_received({Bin, ParsingState}, Callback) of
    ok ->
      NewPendingRequests = handle_pending_requests(PendingRequests),
      NewState = State#state{parsing_state    = void,
                             pending_requests = NewPendingRequests,
                             callback         = undefined},
      {noreply, NewState};
    {incomplete, NewParsingState} ->
      {noreply, State#state{parsing_state = NewParsingState}}
  end;
handle_info(Msg, State) ->
  lager:notice("Unexpected info message received: ~p on ~p", [Msg, State]),
  {noreply, State}.

% Boilerplate
-spec handle_call(any(), any(), state()) -> {reply, ok, state()}.
handle_call(_Msg, _From, State) -> {reply, ok, State}.
-spec terminate(atom(), state()) -> ok.
terminate(_Reason, _State) -> ok.
-spec code_change(string(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%==============================================================================
%% Handlers
%%==============================================================================
init([Options]) ->
  Schema = [{connector, atom, required},
            {client_id, binary, required},
            {max_wait, integer, {default, 1000}},
            {min_bytes, integer, {default, 10}},
            {correlation_id, integer, {default, 0}}],
  case normalizerl:normalize_proplist(Schema, Options) of
    {ok, [ConnectorName, ClientId, MaxWait, MinBytes, CorrelationId]} ->
      kafkerl_connector:add_tcp_listener(ConnectorName, self()),
      {ok, #state{connector_name = ConnectorName, client_id = ClientId,
                  correlation_id = CorrelationId, max_wait = MaxWait,
                  min_bytes = MinBytes}};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("Consumer config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_request_topics(Topics, #state{connector_name = ConnectorName,
                                     client_id = ClientId,
                                     correlation_id = CorrelationId,
                                     max_wait = MaxWait,
                                     min_bytes = MinBytes}) ->
  Req = kafkerl_protocol:build_fetch_request(Topics, ClientId, CorrelationId,
                                             MaxWait, MinBytes),
  kafkerl_connector:send(ConnectorName, Req).

on_binary_received(Bin, Callback) ->
  case parse_fetch_response(Bin) of
    {ok, CorrelationId, Data} ->
      callback(Callback, CorrelationId, get_data_to_send(Data));
    {incomplete, CorrelationId, Data, State} ->
      callback(Callback, CorrelationId, get_data_to_send(Data)),
      {incomplete, State};
    {error, Reason} ->
      lager:error("unable to read kafka message, reason: ~p", [Reason])
  end.

parse_fetch_response({Binary, ParsingState}) when is_atom(ParsingState) ->
  kafkerl_protocol:parse_fetch_response(Binary);
parse_fetch_response({Binary, ParsingState}) ->
  kafkerl_protocol:parse_fetch_response(Binary, ParsingState).

handle_pending_requests(_PendingRequests = []) ->
  [];
handle_pending_requests(_PendingRequests = [{Req, Callback} | Reqs]) ->
  ok = request_topics(Req, Callback),
  Reqs.

%%==============================================================================
%% Utils
%%==============================================================================
callback(_Callback, _CorrelationId, _Data = []) ->
  ok;
callback({M, F}, CorrelationId, Data) ->
  spawn(fun() -> M:F(CorrelationId, Data) end),
  ok;
callback({M, F, A}, CorrelationId, Data) ->
  spawn(fun() -> apply(M, F, A ++ [CorrelationId, Data]) end),
  ok;
callback(Pid, CorrelationId, Data) ->
  Pid ! {kafka_message, CorrelationId, Data},
  ok.

get_data_to_send(RawData) ->
  lists:filtermap(fun nonempty_topic/1, RawData).

nonempty_topic({Name, Partitions}) ->
  case lists:filtermap(fun nonempty_partition/1, Partitions) of
    []    -> false;
    Other -> {true, {Name, Other}}
  end.

nonempty_partition({_Partition, _Messages = []}) ->
  false;
nonempty_partition(Partition) ->
  {true, Partition}.