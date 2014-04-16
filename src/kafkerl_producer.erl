-module(kafkerl_producer).
-author('hernanrivasacosta@gmail.com').
-behavior(gen_server).

-export([send_message/1, send_message/2, send_message/3]).
-export([reconnect/0, reconnect/1]).

-export([start_link/1, start_link/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type config() :: [{compression, kafkerl_compression()} |
                   {id, binary()} |
                   {host, string()} |
                   {port, integer()} |
                   {tcp_options, any()} |
                   {max_retries, integer()}].

% [{id, "hernan-id"}, {host, "localhost"}, {port, 2181}]

-record(state, {config        = [] :: config(),
                socket = undefined :: any(),
                correlationId  = 0 :: integer()}).

-type state() :: #state{}.


-define(DEFAULT_TCP_OPTS, lists:sort([binary, {packet, 0}])).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(config()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Config) ->
  start_link(?MODULE, Config).

-spec start_link(atom(), config()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, Config, []).

-spec send_message(kafkerl_message()) -> ok.
send_message(Data) -> send_message(?MODULE, Data).
-spec send_message(atom(), kafkerl_message()) -> ok;
                  (kafkerl_message(), integer() | infinity) -> ok.
send_message(Name, Data) when is_atom(Name) ->
  send_message(Name, Data, infinity);
send_message(Data, Timeout) ->
  send_message(?MODULE, Data, Timeout).
-spec send_message(atom(), kafkerl_message(), integer() | infinity) -> ok.
send_message(Name, Data, Timeout) ->
  FlatData = case is_list(Data) of
               true -> lists:flatten(Data);
               _    -> [Data]
             end,
  ok = gen_server:call(Name, {message, FlatData}, Timeout).

-spec reconnect() -> ok.
reconnect() -> reconnect(?MODULE).
-spec reconnect(atom() | config()) -> ok.
reconnect(Name) when is_atom(Name) -> reconnect(Name, []);
reconnect(Config) -> reconnect(?MODULE, Config).

-spec reconnect(atom(), config()) -> ok.
reconnect(Name, Config) -> ok = gen_server:call(Name, {reconnect, Config}).

% gen_server callbacks
-spec init(config()) -> {ok, state()} | {error, any()}.
init(Config) ->
  case {lists:keyfind(host, 1, Config), lists:keyfind(port, 1, Config)} of
    {false, _}  -> {error, invalid_config};
    {_, false}  -> {error, invalid_config};
    {{_, Host},
     {_, Port}} -> TCPOpts = proplists:get_value(tcp_options, Config, []),
                   FilteredTCPOpts = get_tcp_options(TCPOpts),
                   {ok, Socket} = gen_tcp:connect(Host, Port, FilteredTCPOpts),
                   {ok, #state{config = Config, socket = Socket}}
  end.

-type valid_call_message() :: {message, kafkerl_message()} |
                              {reconnect, config()}.

-spec handle_call(valid_call_message(), any(), state()) -> {reply, state()}.
handle_call({message, Data}, _From, State) ->
  {reply, ok, handle_send_message(Data, State)};
handle_call({reconnect, Config}, _From, State) ->
  {reply, ok, handle_reconnect(Config, State)}.

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
handle_send_message(Data, State = #state{config = Config, socket = Socket,
                                         correlationId = CorrelationId}) ->
  ClientId = proplists:get_value(id, Config, <<"default_client">>),
  DefaultCompression = ?KAFKERL_COMPRESSION_NONE,
  Compression = proplists:get_value(compression, Config, DefaultCompression),
  Bin = kafkerl_protocol:build_producer_request(Data, ClientId, CorrelationId,
                                              Compression),
  case gen_tcp:send(Socket, Bin) of
    {error, Reason} ->
      lager:warning("Unable to write to socket, reason: ~p", [Reason]),
      % TODO_1: Call reconnect/1 with the correct gen_server name
      % TODO_2: This message will be lost, this should not happen
      reconnect();
    ok ->
      State#state{correlationId = CorrelationId + 1}
  end.

handle_reconnect(Config, State = #state{socket = Socket}) ->
  case connect(Config) of
    {error, Reason} -> lager:critical("unable to reconnect, reason: ~p",
                                      [Reason]),
                       State#state{config = Config};
    {ok, NewSocket} -> lager:info("reconnected successful"),
                       gen_tcp:close(Socket),
                       State#state{socket = NewSocket, config = Config}
  end.

%%==============================================================================
%% Utils
%%==============================================================================
connect(Config) ->
  case {lists:keyfind(host, 1, Config), lists:keyfind(port, 1, Config)} of
    {false, _}  -> {error, invalid_config};
    {_, false}  -> {error, invalid_config};
    {{_, Host},
     {_, Port}} -> TCPOpts = proplists:get_value(tcp_options, Config, []),
                   FilteredTCPOpts = get_tcp_options(TCPOpts),
                   {ok, _Socket} = gen_tcp:connect(Host, Port, FilteredTCPOpts)
  end.

get_tcp_options(Options) -> % TODO: refactor
  lists:ukeymerge(1, lists:sort(proplists:unfold(Options)), ?DEFAULT_TCP_OPTS).