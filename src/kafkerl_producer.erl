-module(kafkerl_producer).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

-export([send_message/1, send_message/2, send_message/3]).

-export([start_link/1, start_link/2, start_link/3]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-record(state, {connector_name = undefined :: atom() | undefined,
                client_id      = undefined :: binary(),
                compression    = undefined :: kafkerl_compression(),
                correlation_id         = 0 :: integer()}).

-type state() :: #state{}.

%%==============================================================================
%% API
%%==============================================================================
-type start_link_response() :: {ok, pid()} | ignore | {error, any()}.

% Starting the server
-spec start_link(kafkerl_conn_config()) -> start_link_response().
start_link(Config) ->
  start_link(Config, []).
-spec start_link(atom(), kafkerl_conn_config()) -> start_link_response();
                (kafkerl_conn_config(), [any()]) -> start_link_response().
start_link(Name, Config) when is_atom(Name) ->
  start_link(Name, Config, []);
start_link(Config, Options) ->
  start_link(?MODULE, Config, Options).
-spec start_link(atom(), kafkerl_conn_config(), [any()]) ->
  start_link_response().
start_link(Name, Config, Options) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config, Options], []).

% Sending messages
-spec send_message(kafkerl_message()) -> ok.
send_message(Data) ->
  send_message(?MODULE, Data).
-spec send_message(atom(), kafkerl_message()) -> ok;
                  (kafkerl_message(), integer() | infinity) -> ok.
send_message(Name, Data) when is_atom(Name) ->
  send_message(Name, Data, infinity);
send_message(Data, Timeout) ->
  send_message(?MODULE, Data, Timeout).
-spec send_message(atom(), kafkerl_message(), integer() | infinity) -> ok.
send_message(Name, Data, Timeout) ->
  gen_server:call(Name, {send_message, Data}, Timeout).

handle_call({send_message, Bin}, _From, State) ->
  {reply, handle_send_message(Bin, State), State}.

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
init([Config, Options]) ->
  Schema = [{connector, atom, optional},
            {client_id, binary, required},
            {compression, atom,
             {default, ?KAFKERL_COMPRESSION_NONE},
             {matches, ?KAFKERL_COMPRESSION_TYPES}},
            {correlation_id, integer, {default, 0}}],
  case normalizerl:normalize_proplist(Schema, Options) of
    {ok, [ConnectorName, ClientId, Compression, CorrelationId]} ->
      case kafkerl_connector:start_link(ConnectorName) of
        {ok, _Pid} ->
          case kafkerl_connector:connect(ConnectorName, Config) of
            ok  -> {ok, #state{connector_name = ConnectorName,
                               client_id = ClientId,
                               compression  = Compression,
                               correlation_id = CorrelationId}};
            Any -> Any
          end;
        Other ->
          Other
      end;
    Error ->
      Error
  end.

handle_send_message(Data, #state{connector_name = ConnectorName,
                                 client_id = ClientId,
                                 compression = Compression,
                                 correlation_id = CorrelationId}) ->
  FlatData = case is_list(Data) of
               true -> lists:flatten(Data);
               _    -> [Data]
             end,
  Req = kafkerl_protocol:build_producer_request(FlatData, ClientId,
                                                CorrelationId, Compression),
  kafkerl_connector:send(ConnectorName, Req).