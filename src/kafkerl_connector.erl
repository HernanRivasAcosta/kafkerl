-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

-export([send/1, send/2]).
-export([add_tcp_listener/1, add_tcp_listener/2]).
-export([remove_tcp_listener/1, remove_tcp_listener/2]).
% Only for internal use
-export([reconnect/6]).

-export([start_link/1, start_link/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-record(state, {host     = undefined :: string() | undefined,
                port     = undefined :: integer() | undefined,
                socket   = undefined :: any() | undefined,
                tcp_options     = [] :: [any()],
                max_retries      = 0 :: integer(),
                retry_interval   = 0 :: integer(),
                reconnecting = false :: boolean(),
                tcp_listeners   = [] :: [pid()]}).

-type state() :: #state{}.
-type start_link_response() :: {ok, pid()} | ignore | {error, any()}.
-type valid_call_message() :: {message, binary()} |
                              {add_tcp_listener, pid()} |
                              {remove_tcp_listener, pid()}.            
-type valid_info_message() :: {tcp_closed, any()} |
                              {tcp, any(), binary()} |
                              {socket_updated, any()} |
                              any().

-define(DEFAULT_TCP_OPTS, lists:sort([binary, {packet, 0}])).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(kafkerl_conn_config()) -> start_link_response().
start_link(Config) ->
  start_link(?MODULE, Config).
-spec start_link(atom(), kafkerl_conn_config()) -> start_link_response().
start_link(undefined, Config) ->
  start_link(Config);
start_link(Name, Config) ->
  gen_server:start_link({local, Name}, ?MODULE, [Config], []).

-spec send(binary()) -> ok | {error, any()}.
send(Bin) ->
  send(?MODULE, Bin).
-spec send(atom(), binary()) -> ok | {error, any()};
          (binary(), integer() | infinity) -> ok | {error, any()}.
send(undefined, Bin) ->
  send(?MODULE, Bin, infinity);
send(Name, Bin) when is_atom(Name) ->
  send(Name, Bin, infinity);
send(Bin, Timeout) ->
  send(?MODULE, Bin, Timeout).
-spec send(atom(), binary(), integer() | infinity) -> ok | {error, any()}.
send(Name, Bin, Timeout) ->
  gen_server:call(Name, {send, Bin}, Timeout).

-spec add_tcp_listener(pid()) -> ok.
add_tcp_listener(Pid) ->
  add_tcp_listener(?MODULE, Pid).
-spec add_tcp_listener(atom(), pid()) -> ok.
add_tcp_listener(Name, Pid) ->
  gen_server:call(Name, {add_tcp_listener, Pid}).

-spec remove_tcp_listener(pid()) -> ok.
remove_tcp_listener(Pid) ->
  remove_tcp_listener(?MODULE, Pid).
-spec remove_tcp_listener(atom(), pid()) -> ok.
remove_tcp_listener(Name, Pid) ->
  gen_server:call(Name, {remove_tcp_listener, Pid}).

% gen_server callbacks
-spec handle_call(valid_call_message(), any(), state()) ->
  {reply, ok, state()} |
  {reply, {error, any(), state()}}.
handle_call({send, Bin}, _From, State) ->
  {reply, handle_send(Bin, State), State};
handle_call({add_tcp_listener, Pid}, _From,
            State = #state{tcp_listeners = Listeners}) ->
  NewListeners = case lists:member(Pid, Listeners) of
                   true  -> Listeners;
                   false -> [Pid | Listeners]
                 end,
  {reply, ok, State#state{tcp_listeners = NewListeners}};
handle_call({remove_tcp_listener, Pid}, _From,
            State = #state{tcp_listeners = Listeners}) ->
  NewListeners = case lists:member(Pid, Listeners) of
                   true  -> Listeners;
                   false -> [Pid | Listeners]
                 end,
  {reply, ok, State#state{tcp_listeners = NewListeners}}.

-spec handle_info(valid_info_message(), state()) -> {noreply, state()}.
handle_info({tcp_closed, _Socket},
            State = #state{host = Host, port = Port, tcp_options = TCPOpts,
                           max_retries = MaxRetries,
                           retry_interval = RetryInterval}) ->
  lager:warning("lost connection to kafka server at ~s:~p, reconnecting",
                [Host, Port]),
  Params = [self(), Host, Port, TCPOpts, MaxRetries, RetryInterval],
  _Pid = spawn_link(?MODULE, reconnect, Params),
  {noreply, State#state{socket = undefined, reconnecting = true}};
handle_info({tcp, _Socket, Bin}, State = #state{tcp_listeners = Listeners}) ->
  Message = {kafka_message, Bin},
  lists:foreach(fun(Pid) -> Pid ! Message end, Listeners),
  {noreply, State};
handle_info({socket_updated, undefined}, State = #state{socket = undefined}) ->
  {stop, {error, unable_to_reconnect}, State};
handle_info({socket_updated, Socket}, State) ->
  {noreply, State#state{socket = Socket, reconnecting = false}};
handle_info(Msg, State) ->
  lager:notice("Unexpected info message received: ~p on ~p", [Msg, State]),
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
init([Config]) ->
  Schema = [{host, string, required},
            {port, integer, required},
            {tcp_options, {list, any}, {default, []}},
            {max_retries, integer, {default, -1}},
            {retry_interval, integer, {default, 1000}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [Host, Port, TCPOpts, MaxRetries, RetryInterval]} ->
      State = #state{host = Host, port = Port, tcp_options = TCPOpts,
                     max_retries = MaxRetries, retry_interval = RetryInterval},
      case do_connect(Host, Port, TCPOpts) of
        {ok, Socket} ->
          {ok, State#state{socket = Socket}};
        _Error ->
          Params = [self(), Host, Port, TCPOpts, -1, RetryInterval],
          _Pid = spawn_link(?MODULE, reconnect, Params),
          {ok, State#state{reconnecting = true}}
      end;
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("Connector config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_send(_Bin, #state{socket = undefined}) ->
  % Maybe cache the binary
  lager:info("ignoring send"),
  ok;
handle_send(Bin, #state{socket = Socket}) ->
  case gen_tcp:send(Socket, Bin) of
    {error, Reason} ->
      lager:warning("Unable to write to socket, reason: ~p", [Reason]),
      {error, Reason};
    ok ->
      ok
  end.

%%==============================================================================
%% Utils
%%==============================================================================
get_tcp_options(Options) -> % TODO: refactor
  lists:ukeymerge(1, lists:sort(proplists:unfold(Options)), ?DEFAULT_TCP_OPTS).

do_connect(Host, Port, TCPOpts) ->
  gen_tcp:connect(Host, Port, get_tcp_options(TCPOpts)).

reconnect(Pid, _Host, _Port, _TCPOpts, 0, _RetryInterval) ->
  Pid ! {socket_updated, undefined};
reconnect(Pid, Host, Port, TCPOpts, Retries, RetryInterval) ->
  case do_connect(Host, Port, TCPOpts) of
    {error, Reason} ->
      lager:warning("unable to connect to kafka server, reason: ~p", [Reason]),
      timer:sleep(RetryInterval),
      NewRetries = Retries - 1,
      reconnect(Pid, Host, Port, TCPOpts, NewRetries, RetryInterval);
    {ok, Socket} ->
      lager:info("reconnected to kafka server at ~s:~p", [Host, Port]),
      gen_tcp:controlling_process(Socket, Pid),
      Pid ! {socket_updated, Socket}
  end.