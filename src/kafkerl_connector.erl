-module(kafkerl_connector).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_server).

%% API
% Metadata
-export([request_metadata/0, request_metadata/1, get_partitions/0]).
% Produce
-export([send/1]).
% Consume
-export([fetch/3, stop_fetch/2]).
% Common
-export([subscribe/1, subscribe/2, unsubscribe/1]).
% Only used by broker connections
-export([produce_succeeded/1]).
% Only used by the metadata handler
-export([topic_mapping_updated/1]).
% Supervisors
-export([start_link/1]).

-export([get_dump_files/0]).
% gen_server callbacks
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-include("kafkerl.hrl").

-type kafler_host() :: string().
-type kafler_port() :: 1..65535.
-type address()     :: {kafler_host(), kafler_port()}.

-type broker_mapping_key()  :: {kafkerl:topic(), kafkerl:partition()}.
-type broker_mapping()      :: {broker_mapping_key(), kafkerl:server_ref()}.

-record(state, {broker_mapping      = void :: [broker_mapping()] | void,
                config                = [] :: [{atom(), any()}],
                autocreate_topics  = false :: boolean(),
                callbacks             = [] :: [{kafkerl:filters(),
                                                kafkerl:callback()}],
                last_dump_name   = {"", 0} :: {string(), integer()},
                default_fetch_options = [] :: kafkerl:options(),
                dump_location         = "" :: string(),
                max_buffer_size        = 0 :: integer(),
                save_bad_messages  = false :: boolean()}).
-type state() :: #state{}.

-export_type([address/0]).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(any()) -> {ok, pid()} | ignore | kafkerl:error().
start_link(Config) ->
  gen_server:start_link({local, kafkerl}, ?MODULE, [Config], []).

-spec send(kafkerl:basic_message()) -> kafkerl:ok() | kafkerl:error().
send({Topic, Partition, _Payload} = Message) ->
  Buffer = kafkerl_utils:buffer_name(Topic, Partition),
  case ets_buffer:write(Buffer, Message) of
    NewSize when is_integer(NewSize) ->
      % Return 'saved' when the message went to the right ETS
      {ok, saved};
    Error ->
      _ = lager:debug("error writing on ~p, reason: ~p", [Buffer, Error]),
      case ets_buffer:write(kafkerl_utils:default_buffer_name(), Message) of
        NewDefaultBufferSize when is_integer(NewDefaultBufferSize) ->
          % We return 'cached' when we needed to use the default ets table
          {ok, cached};
        _ ->
          _ = lager:critical("unable to write to default buffer, the message ~p"
                             " was lost lost, reason: ~p",
                             [Message, Error]),
          % We can safely assume that the ets existance indicates if kafkerl was
          % started
          {error, not_started}
      end
  end.

-spec fetch(kafkerl:topic(), kafkerl:partition(), kafkerl:options()) ->
  ok | kafkerl:error().
fetch(Topic, Partition, Options) ->
  gen_server:call(kafkerl, {fetch, Topic, Partition, Options}).

-spec stop_fetch(kafkerl:topic(), kafkerl:partition()) -> ok.
stop_fetch(Topic, Partition) ->
  gen_server:call(kafkerl, {stop_fetch, Topic, Partition}).

-spec get_partitions() -> [{kafkerl:topic(), [kafkerl:partition()]}] |
                          kafkerl:error().
get_partitions() ->
  case gen_server:call(kafkerl, {get_partitions}) of
    {ok, Mapping} ->
      get_partitions_from_mapping(Mapping);
    Error ->
      Error
  end.
-spec get_dump_files() -> {ok, any()} | {error, any()}.
get_dump_files() ->
    gen_server:call(kafkerl, get_dump_files).

-spec subscribe(kafkerl:callback()) -> ok | kafkerl:error().
subscribe(Callback) ->
  subscribe(Callback, all).
-spec subscribe(kafkerl:callback(), kafkerl:filters()) -> ok | kafkerl:error().
subscribe(Callback, Filters) ->
  gen_server:call(kafkerl, {subscribe, {Filters, Callback}}).

-spec unsubscribe(kafkerl:callback()) -> ok.
unsubscribe(Callback) ->
  gen_server:call(kafkerl, {unsubscribe, Callback}).

-spec request_metadata() -> ok.
request_metadata() ->
  gen_server:call(kafkerl, {request_metadata, []}).

-spec request_metadata([kafkerl:topic()]) -> ok.
request_metadata(Topics) ->
  gen_server:call(kafkerl, {request_metadata, Topics}).

-spec produce_succeeded([{kafkerl:topic(), kafkerl:partition(),
                          [binary()], integer()}]) -> ok.
produce_succeeded(Produced) ->
  gen_server:cast(kafkerl, {produce_succeeded, Produced}).

-spec topic_mapping_updated(any()) -> ok.
topic_mapping_updated(TopicMapping) ->
  gen_server:cast(kafkerl, {topic_mapping_updated, TopicMapping}).

%%==============================================================================
%% gen_server callbacks
%%==============================================================================
-spec handle_call(any(), any(), state()) -> {reply, ok, state()} |
                                            {reply, {error, any()}, state()}.
handle_call({fetch, Topic, Partition, Options}, _From, State) ->
  {reply, handle_fetch(Topic, Partition, Options, State), State};
handle_call({stop_fetch, Topic, Partition}, _From, State) ->
  {reply, handle_stop_fetch(Topic, Partition, State), State};
handle_call({request_metadata, Topics}, _From, State) ->
  {reply, handle_request_metadata(Topics), State};
handle_call({get_partitions}, _From, State) ->
  {reply, handle_get_partitions(State), State};
handle_call({subscribe, Callback}, _From, State) ->
  case send_mapping_to(Callback, State) of
    ok ->
      {reply, ok, State#state{callbacks = [Callback | State#state.callbacks]}};
    _  ->
      {reply, {error, invalid_callback}, State}
  end;
handle_call({unsubscribe, Callback}, _From, State) ->
  NewCallbacks = lists:keydelete(Callback, 2, State#state.callbacks),
  {reply, ok, State#state{callbacks = NewCallbacks}};

handle_call(get_dump_files, _From, State) ->
    DumpLocation = State#state.dump_location,
    WorkingDirectory = case file:get_cwd() of
                           {ok, Path} -> Path;
                           {error, _} -> ""
                       end,
    FilePath = filename:join([WorkingDirectory, DumpLocation]),
    case file:list_dir(FilePath) of
        {ok, Filenames} ->
            {reply, {ok, [FilePath ++ F || F <- Filenames, lists:suffix(".dump", F)]}, State};
        Error ->
            {reply, Error, State}
    end.

-spec handle_info(any(), state()) -> {noreply, state()} |
                                     {stop, {error, any()}, state()}.
handle_info(dump_buffer_tick, State) ->
  {noreply, handle_dump_buffer_to_disk(State)};
handle_info(metadata_timeout, State) ->
  {stop, {error, unable_to_retrieve_metadata}, State};
handle_info({metadata_updated, []}, State) ->
  % If the metadata arrived empty request it again
  ok = handle_request_metadata([]),
  {noreply, State};
%handle_info({metadata_updated, Mapping}, State) ->

handle_info(Msg, State) ->
  _ = lager:notice("Unexpected info message received: ~p on ~p", [Msg, State]),
  {noreply, State}.

-spec handle_cast(any(), state()) -> {noreply, state()}.
handle_cast({produce_succeeded, Produced}, State) ->
  Callbacks = State#state.callbacks,
  NewCallbacks = send_event({produced, Produced}, Callbacks),
  {noreply, State#state{callbacks = NewCallbacks}};
handle_cast({topic_mapping_updated, NewMapping}, State) ->
  % Get the partition data to send to the subscribers and send it
  PartitionData = get_partitions_from_mapping(NewMapping),
  Callbacks = State#state.callbacks,
  NewCallbacks = send_event({partition_update, PartitionData}, Callbacks),
  {noreply, State#state{callbacks = NewCallbacks, broker_mapping = NewMapping}}.

% Boilerplate
-spec terminate(atom(), state()) -> ok.
terminate(_Reason, _State) -> ok.
-spec code_change(string(), state(), any()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%==============================================================================
%% Handlers
%%==============================================================================
init([Config]) ->
  % The schema indicates what is expected of the configuration, it validates and
  % normalizes the configuration
  Schema = [{assume_autocreate_topics, boolean, {default, false}},
            {consumer_min_bytes, positive_integer, {default, 1}},
            {consumer_max_wait, positive_integer, {default, 1500}},
            {dump_location, string, {default, ""}},
            {max_buffer_size, positive_integer, {default, 500}},
            {save_messages_for_bad_topics, boolean, {default, true}},
            {flush_to_disk_every, positive_integer, {default, 10000}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [AutocreateTopics, MinBytes, MaxWait, DumpLocation,
          MaxBufferSize, SaveBadMessages, FlushToDiskInterval]} ->
      % Start the metadata request handler
      {ok, _} = kafkerl_metadata_handler:start_link(Config),
      % Build the default fetch options
      DefaultFetchOptions = [{min_bytes, MinBytes}, {max_wait, MaxWait}],
      State = #state{config                = Config,
                     dump_location         = DumpLocation,
                     max_buffer_size       = MaxBufferSize,
                     save_bad_messages     = SaveBadMessages,
                     autocreate_topics     = AutocreateTopics,
                     default_fetch_options = DefaultFetchOptions},
      % Create a buffer to hold unsent messages
      _ = kafkerl_buffer:create_buffer(kafkerl_utils:default_buffer_name(), fifo),
      % Start the interval that manages the buffers holding unsent messages
      {ok, _TRef} = timer:send_interval(FlushToDiskInterval, dump_buffer_tick),
      ok = kafkerl_metadata_handler:request_metadata([]),
      {ok, State};
    {errors, Errors} ->
      ok = lists:foreach(fun(E) ->
                           _ = lager:critical("Connector config error ~p", [E])
                         end, Errors),
      {stop, bad_config}
  end.

handle_fetch(_Topic, _Partition, _Options, #state{broker_mapping = void}) ->
  {error, not_connected};
handle_fetch(Topic, Partition, Options, State) ->
  case lists:keyfind({Topic, Partition}, 1, State#state.broker_mapping) of
    false ->
      {error, {no_broker, {Topic, Partition}}};
    {_, Broker} ->
      NewOptions = Options ++ State#state.default_fetch_options,
      kafkerl_broker_connection:fetch(Broker, Topic, Partition, NewOptions)
  end.

handle_stop_fetch(_Topic, _Partition, #state{broker_mapping = void}) ->
  % Ignore, there's no fetch in progress
  ok;
handle_stop_fetch(Topic, Partition, State) ->
  case lists:keyfind({Topic, Partition}, 1, State#state.broker_mapping) of
    false ->
      % Ignore, there's no fetch in progress
      ok;
    {_, Broker} ->
      kafkerl_broker_connection:stop_fetch(Broker, Topic, Partition)
  end.

handle_get_partitions(#state{broker_mapping = void}) ->
  {error, not_available};
handle_get_partitions(#state{broker_mapping = Mapping}) ->
  {ok, Mapping}.

handle_request_metadata(Topics) ->
  kafkerl_metadata_handler:request_metadata(Topics).

handle_dump_buffer_to_disk(State = #state{dump_location = DumpLocation,
                                          last_dump_name = LastDumpName}) ->
  % Get the buffer name and all the messages from it
  Buffer = kafkerl_utils:default_buffer_name(),
  MessagesInBuffer = ets_buffer:read_all(Buffer),
  % Split them between the ones that should be retried and those that don't
  {ToDump, ToRetry} = split_message_dump(MessagesInBuffer, State),
  % Retry the messages on an async function (to avoid locking this gen_server)
  ok = retry_messages(ToRetry),
  % And dump the messages that need to be dumped into a file
  case ToDump of
    [_ | _] = Messages ->
      % Get the name of the file we want to write to
      {DumpNameStr, _} = NewDumpName = get_ets_dump_name(LastDumpName),
      % Build the location
      WorkingDirectory = case file:get_cwd() of
                           {ok, Path} -> Path;
                           {error, _} -> ""
                         end,
      FilePath = filename:join([WorkingDirectory, DumpLocation, DumpNameStr]),
      % Write to disk
      _ = case file:write_file(FilePath, term_to_binary(Messages)) of
            ok ->
              lager:info("Dumped unsent messages at ~p", [FilePath]);
            Error ->
              lager:critical("Unable to save messages, reason: ~p", [Error])
          end,
      State#state{last_dump_name = NewDumpName};
    _ ->
      State
  end.

%%==============================================================================
%% Utils
%%==============================================================================
retry_messages([]) ->
  ok;
retry_messages(Messages) ->
  _Pid = spawn(fun() -> [send(M) || M <- Messages] end),
  ok.

split_message_dump(Messages, #state{max_buffer_size = MaxBufferSize,
                                    save_bad_messages = SaveBadMessages})
  when is_list(Messages) ->

  KnownTopics = kafkerl_metadata_handler:get_known_topics(),
  % Split messages between for topics kafkerl knows exist and those that do not.
  {Known, Unknown} = lists:partition(fun({Topic, _Partition, _Payload}) ->
                                       lists:member(Topic, KnownTopics)
                                     end, Messages),
  % The messages to be dumped are those from unkown topics (if the settings call
  % for it) and those from known topics if the buffer size is too large.
  % The messages to be retried are those from the known topics, as long as their
  % number does not exceed the MaxBufferSize.
  case {SaveBadMessages, length(Known) >= MaxBufferSize} of
    {true, true} ->
      {Unknown ++ Known, []};
    {false, true} ->
      {Known, []};
    {true, false} ->
      {Unknown, Known};
    {false, false} ->
      {[], Known}
  end;
% If the messages are not a list, then it's an ets error, report it and move on.
% And yes, those messages are gone forever
split_message_dump(Error, _State) ->
  lager:error("Unable to get messages from buffer, reason: ~p", [Error]),
  {[], []}.

get_ets_dump_name({OldName, Counter}) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:local_time(),
  Ts = io_lib:format("~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B_",
                     [Year, Month, Day, Hour, Minute, Second]),
  PartialNewName = "kafkerl_messages_" ++ lists:flatten(Ts),
  case lists:prefix(PartialNewName, OldName) of
    true ->
      {PartialNewName ++ integer_to_list(Counter) ++ ".dump", Counter + 1};
    _ ->
      {PartialNewName ++ "0.dump", 1}
  end.

send_event(Event, {all, Callback}) ->
  kafkerl_utils:send_event(Callback, Event);
send_event({EventName, _Data} = Event, {Events, Callback}) ->
  case is_list(Events) andalso lists:member(EventName, Events) of
    true -> kafkerl_utils:send_event(Callback, Event);
    false -> ok
  end;
send_event(Event, Callbacks) ->
  lists:filter(fun(Callback) ->
                 send_event(Event, Callback) =:= ok
               end, Callbacks).

% This is used to return the available partitions for each topic
get_partitions_from_mapping(Mapping) ->
  F = fun({{Topic, Partition}, _}, Acc) ->
        case lists:keytake(Topic, 1, Acc) of
          false ->
            [{Topic, [Partition]} | Acc];
          {value, {Topic, Partitions}, NewAcc} ->
            [{Topic, [Partition | Partitions]} | NewAcc]
        end
      end,
  lists:foldl(F, [], Mapping).

send_mapping_to(_NewCallback, #state{broker_mapping = void}) ->
  ok;
send_mapping_to(NewCallback, #state{broker_mapping = Mapping}) ->
  Partitions = get_partitions_from_mapping(Mapping),
  send_event({partition_update, Partitions}, NewCallback).