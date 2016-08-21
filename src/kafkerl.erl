-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0]).
-export([produce/3, produce/4, produce/5,
         consume/2, consume/3, consume/4, stop_consuming/2, stop_consuming/3,
         request_metadata/0, request_metadata/1, request_metadata/2,
         partitions/0, partitions/1]).
-export([version/0]).

%% Types
-type offset()     :: integer().

-type callback()   :: pid() |
                      fun() |
                      {atom(), atom()} |
                      {atom(), atom(), [any()]}.
-type option()     :: {buffer_size, integer() | infinity} |
                      {dump_location, string()} |
                      {consumer, callback()} |
                      {min_bytes, integer()} |
                      {max_wait, integer()} |
                      {offset, offset()} |
                      {fetch_interval, false | integer()}.
-type options()    :: [option()].
-type server_ref() :: atom() | pid().

-type error() :: {error, atom() | {atom(), any()}}.

-type topic()     :: binary().
-type partition() :: integer().
-type payload()   :: binary() | [binary()].
-type basic_message()   :: {topic(), partition(), payload()}.

-export_type([server_ref/0, error/0, options/0, callback/0,
              topic/0, partition/0, payload/0, basic_message/0]).

%%==============================================================================
%% API
%%==============================================================================
-spec start() -> ok | {error, term()}.
start() ->
  ok = application:load(?MODULE),
  application:start(?MODULE).

%%==============================================================================
%% Access API
%%==============================================================================
%% Produce API

-spec produce(server_ref(), basic_message(), options()) -> ok;
             (topic(), partition(), payload()) -> ok.
produce(_ServerRef, {Topic, Partition, Message}, Options) ->
  produce(?MODULE, Topic, Partition, Message, Options);
produce(Topic, Partition, Message) ->
  produce(?MODULE, Topic, Partition, Message, []).

-spec produce(server_ref(), topic(), partition(), payload()) -> ok;
             (topic(), partition(), payload(), options()) -> ok.
produce(Topic, Partition, Message, Options) when is_list(Options) ->
  produce(?MODULE, {Topic, Partition, Message}, Options);
produce(ServerRef, Topic, Partition, Message) ->
  produce(ServerRef, {Topic, Partition, Message}, []).

-spec produce(server_ref(), topic(), partition(), payload(), options()) -> ok.
produce(ServerRef, Topic, Partition, Message, Options) ->
  kafkerl_connector:send(ServerRef, {Topic, Partition, Message}, Options).

%% Consume API
-spec consume(topic(), partition()) -> ok | error().
consume(Topic, Partition) ->
  consume(?MODULE, Topic, Partition, []).

-spec consume(topic(), partition(), options()) -> ok | error();
             (server_ref(), topic(), partition()) -> ok | error().
consume(Topic, Partition, Options) when is_list(Options) ->
  consume(?MODULE, Topic, Partition, Options);
consume(ServerRef, Topic, Partition) ->
  consume(ServerRef, Topic, Partition, []).

-spec consume(server_ref(), topic(), partition(), options()) ->
  ok | {[payload()], offset()} | error().
consume(ServerRef, Topic, Partition, Options) ->
  case {proplists:get_value(consumer, Options, undefined),
        proplists:get_value(fetch_interval, Options, false)} of
    {undefined, false} ->
      NewOptions = [{consumer, self()} | Options],
      ok = kafkerl_connector:fetch(ServerRef, Topic, Partition, NewOptions),
      kafkerl_utils:gather_consume_responses();
    {undefined, _} ->
      {error, fetch_interval_specified_with_no_consumer};
    _ ->
      kafkerl_connector:fetch(ServerRef, Topic, Partition, Options)
  end.

-spec stop_consuming(topic(), partition()) -> ok.
stop_consuming(Topic, Partition) ->
  stop_consuming(?MODULE, Topic, Partition).

-spec stop_consuming(server_ref(), topic(), partition()) -> ok.
stop_consuming(ServerRef, Topic, Partition) ->
  kafkerl_connector:stop_fetch(ServerRef, Topic, Partition).

%% Metadata API
-spec request_metadata() -> ok.
request_metadata() ->
  request_metadata(?MODULE).

-spec request_metadata(atom() | [topic()]) -> ok.
request_metadata(Topics) when is_list(Topics) ->
  request_metadata(?MODULE, Topics);
request_metadata(ServerRef) ->
  kafkerl_connector:request_metadata(ServerRef).

-spec request_metadata(atom(), [topic()]) -> ok.
request_metadata(ServerRef, Topics) ->
  kafkerl_connector:request_metadata(ServerRef, Topics).

%% Partitions
-spec partitions() -> [{topic(), [partition()]}] | error().
partitions() ->
  partitions(?MODULE).

-spec partitions(server_ref()) -> [{topic(), [partition()]}] | error().
partitions(ServerRef) ->
  kafkerl_connector:get_partitions(ServerRef).

%% Utils
-spec version() -> {integer(), integer(), integer()}.
version() ->
  {2, 0, 0}.