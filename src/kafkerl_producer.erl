-module(kafkerl_producer).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

-export([send_message/1, send_message/2, send_message/3]).

-export([start_link/1, start_link/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-record(state, {connector_name = undefined :: atom() | undefined,
                client_id      = undefined :: binary(),
                compression    = undefined :: kafkerl_compression(),
                correlation_id         = 0 :: integer()}).

-type state() :: #state{}.
-type start_link_response() :: {ok, pid()} | ignore | {error, any()}.
-type valid_call_message() :: {send_message, kafkerl_message()}.
% What is and isn't valid is described on the schema in the init function
-type valid_producer_option() :: {connector, any()} |
                                 {client_id, any()} |
                                 {compression, any()} |
                                 {correlation_id, any()}.

%%==============================================================================
%% API
%%==============================================================================
% Starting the server
-spec start_link(any()) -> start_link_response().
start_link(Options) ->
  start_link(?MODULE, Options).
-spec start_link(atom(), any()) -> start_link_response().
start_link(Name, Options) when is_atom(Name) ->
  gen_server:start_link({local, Name}, ?MODULE, {Options}, []).
  
% Sending messages
-spec send_message(kafkerl_message()) -> ok.
send_message(Data) ->
  send_message(?MODULE, Data).
-spec send_message(atom(), kafkerl_message()) -> ok;
                  (kafkerl_message(), integer()) -> ok.
send_message(Name, Data) when is_atom(Name) ->
  send_message(Name, Data, 5000);
send_message(Data, Timeout) ->
  send_message(?MODULE, Data, Timeout).
-spec send_message(atom(), kafkerl_message(), integer()) -> ok.
send_message(Name, Data, Timeout) ->
  gen_server:call(Name, {send_message, Data}, Timeout).

% gen_server callbacks
-spec handle_call(valid_call_message(), any(), state()) ->
  {reply, ok, state()} |
  {reply, {error, any(), state()}}.
handle_call({send_message, Message}, _From,
            State = #state{correlation_id = CorrelationId}) ->
  case handle_send_message(Message, State) of
    ok ->
      NewState = State#state{correlation_id = CorrelationId + 1},
      {reply, ok, NewState};
    Error ->
      {reply, Error, State}
  end.

% Boilerplate
-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) -> {noreply, State}.
-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info(_Msg, State) -> {noreply, State}.
-spec terminate(atom(), state()) -> ok.
terminate(_Reason, _State) -> ok.
-spec code_change(string(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%==============================================================================
%% Handlers
%%==============================================================================
-spec init({[valid_producer_option()]}) -> {ok, state()} | {stop, any()}.
init({Options}) ->
  Schema = [{connector, atom, required},
            {client_id, binary, required},
            {compression, atom,
             {default, ?KAFKERL_COMPRESSION_NONE},
             {matches, ?KAFKERL_COMPRESSION_TYPES}},
            {correlation_id, integer, {default, 0}}],
  case normalizerl:normalize_proplist(Schema, Options) of
    {ok, [ConnectorName, ClientId, Compression, CorrelationId]} ->
      {ok, #state{connector_name = ConnectorName, client_id = ClientId,
                  compression = Compression, correlation_id = CorrelationId}};
    {errors, Errors} ->
      ok = lists:foreach(fun(E) ->
                           lager:critical("Producer config error ~p", [E])
                         end, Errors),
      {stop, bad_config}
  end.

handle_send_message(Data, #state{connector_name = ConnectorName,
                                 client_id = ClientId,
                                 compression = Compression,
                                 correlation_id = CorrelationId}) ->
  FlatData = case is_list(Data) of
               true -> lists:flatten(Data);
               _    -> [Data]
             end,
  Req = kafkerl_protocol:build_produce_request(FlatData, ClientId,
                                               CorrelationId, Compression),
  kafkerl_connector:send(ConnectorName, Req).