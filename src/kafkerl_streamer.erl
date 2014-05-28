-module(kafkerl_streamer).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

-export([begin_stream/2, begin_stream/3, begin_stream/4]).
% Only for internal use
-export([stream_received/3, on_timeout/1]).

-export([start_link/1, start_link/2]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

-type stream_request()      :: {kafkerl_topic(), kafkerl_partition()}.

-record(state, {consumer  = undefined :: atom() | undefined,
                offset            = 0 :: integer(),
                request   = undefined :: stream_request(),
                callback  = undefined :: kafkerl_callback() | undefined,
                timeout        = 1000 :: integer(),
                timer_ref = undefined :: any() | undefined, %% any() is a tref()
                name      = undefined :: atom()}).

-type state() :: #state{}.
-type start_link_response() :: {ok, pid()} | ignore | {error, any()}.
-type valid_cast_message()  :: {begin_stream, stream_request(),
                                kafkerl_callback(), integer()} |
                               on_timeout.


%%==============================================================================
%% API
%%==============================================================================
% Starting the server
-spec start_link(any()) -> start_link_response().
start_link(Options) ->
  start_link(?MODULE, Options).

-spec start_link(atom(), any()) -> start_link_response().
start_link(Name, Options) ->
  gen_server:start_link({local, Name}, ?MODULE, [Name, Options], []).

% Request stream
-spec begin_stream(stream_request(), kafkerl_callback()) -> ok.
begin_stream(Request, Callback) ->
  begin_stream(Request, Callback, 0).

-spec begin_stream(atom(), stream_request(), kafkerl_callback()) -> ok;
                  (stream_request(), kafkerl_callback(), integer()) -> ok.
begin_stream(Name, Request, Callback) when is_atom(Name) ->
  begin_stream(Name, Request, Callback, 0);
begin_stream(Request, Callback, Offset) ->
  begin_stream(?MODULE, Request, Callback, Offset).

-spec begin_stream(atom(), stream_request(), kafkerl_callback(), integer()) ->
  ok.
begin_stream(Name, Request, Callback, Offset) ->
  gen_server:cast(Name, {begin_stream, Request, Callback, Offset}).

% Called by the consumer
-spec stream_received(atom(), kafkerl_message_metadata(), any()) -> ok.
stream_received(Name, Metadata, Data) ->
  gen_server:cast(Name, {stream_received, Metadata, Data}).

% Called by the timer
-spec on_timeout(atom()) -> ok.
on_timeout(Name) ->
  gen_server:cast(Name, on_timeout).

% gen_server callbacks
-spec handle_cast(valid_cast_message(), state()) -> {noreply, state()}.
handle_cast({begin_stream, Request, Callback, Offset}, State) ->
  ok = handle_begin_stream(Request, State),
  NewState = State#state{offset = Offset, request = Request,
                         callback = Callback},
  {noreply, NewState};
handle_cast({stream_received, {Status, OffsetData, _CorrelationId}, Data},
            State = #state{callback = Callback, timer_ref = TRef,
                           offset = OldOffset}) ->
  kafkerl_utils:callback(Callback, format_data(Data)),
  NewTRef = case handle_start_timer(Status, State) of
              void  -> TRef;
              Other -> Other
            end,
  Offset = case OffsetData of
             [{_, [NewOffset]}] -> NewOffset;
             _                  -> OldOffset
           end,
  {noreply, State#state{offset = Offset, timer_ref = NewTRef}};
handle_cast(on_timeout, State = #state{request = Request}) ->
  handle_begin_stream(Request, State),
  {noreply, State}.

% Boilerplate
-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info(_Msg, State) -> {noreply, State}.
-spec handle_call(any(), any(), state()) -> {reply, ok, state()}.
handle_call(_Msg, _From, State) -> {reply, ok, State}.
-spec terminate(atom(), state()) -> ok.
terminate(_Reason, _State) -> ok.
-spec code_change(string(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%==============================================================================
%% Handlers
%%==============================================================================
init([Name, Options]) ->
  Schema = [{consumer, atom, required},
            {timeout, integer, {default, 1000}}],
  case normalizerl:normalize_proplist(Schema, Options) of
    {ok, [ConsumerName, Timeout]} ->
      {ok, #state{consumer = ConsumerName, timeout = Timeout, name = Name}};
    {errors, Errors} ->
      lists:foreach(fun(E) ->
                      lager:critical("Streamer config error ~p", [E])
                    end, Errors),
      {stop, bad_config}
  end.

handle_begin_stream({Topic, Partition}, #state{name = Name,
                                               offset = Offset,
                                               consumer = Consumer}) ->
  Callback = {?MODULE, stream_received, [Name]},
  ConsumerRequest = {Topic, {Partition, Offset, 2147483647}},
  ok = kafkerl_consumer:request_topics(Consumer, ConsumerRequest, Callback).

handle_start_timer(done, #state{name = Name, timeout = Timeout}) ->
  {ok, TRef} = timer:apply_after(Timeout, ?MODULE, on_timeout, [Name]),
  TRef;
handle_start_timer(incomplete, _State) ->
  void.

%%==============================================================================
%% Utils
%%==============================================================================
format_data([]) ->
  [];
format_data([{_, [{_, Data}]}]) ->
  Data.