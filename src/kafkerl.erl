-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0, start/2]).
-export([produce/3,
         consume/2, consume/3, stop_consuming/2,
         request_metadata/0, request_metadata/1,
         partitions/0]).
-export([version/0]).

%% Types
-type offset()     :: integer().

-type callback()   :: pid() |
                      fun() | 
                      {atom(), atom()} |
                      {atom(), atom(), [any()]}.
-type option()     :: {buffer_size, integer() | infinity} | 
                      {consumer, callback()} |
                      {min_bytes, integer()} |
                      {max_wait, integer()} |
                      {offset, offset()} |
                      {fetch_interval, false | integer()}.
-type options()    :: [option()].
-type server_ref() :: atom() | pid().

-type ok()    :: {ok, atom()}.
-type error() :: {error, atom() | {atom(), any()}}.

-type topic()     :: binary().
-type partition() :: integer().
-type payload()   :: binary() | [binary()].
-type basic_message()   :: {topic(), partition(), payload()}.

-export_type([server_ref/0, error/0, options/0, topic/0, partition/0, payload/0,
              callback/0, basic_message/0]).

%%==============================================================================
%% API
%%==============================================================================
start() ->
  application:load(?MODULE),
  application:start(?MODULE).

start(_StartType, _StartArgs) ->
  kafkerl_sup:start_link().

%%==============================================================================
%% Access API
%%==============================================================================
%% Produce API
-spec produce(topic(), partition(), payload()) -> ok() | error().
produce(Topic, Partition, Message) ->
  kafkerl_connector:send({Topic, Partition, Message}).
  
%% Consume API
-spec consume(topic(), partition()) -> ok | error().
consume(Topic, Partition) ->
  consume(Topic, Partition, []).

-spec consume(topic(), partition(), options()) -> ok |
                                                  {[payload()], offset()} |
                                                  error().
consume(Topic, Partition, Options) ->
  case {proplists:get_value(consumer, Options, undefined),
        proplists:get_value(fetch_interval, Options, false)} of
    {undefined, false} ->
      NewOptions = [{consumer, self()} | Options],
      kafkerl_connector:fetch(Topic, Partition, NewOptions),
      kafkerl_utils:gather_consume_responses();
    {undefined, _} ->
      {error, fetch_interval_specified_with_no_consumer};
    _ ->
      kafkerl_connector:fetch(Topic, Partition, Options)
  end.

-spec stop_consuming(topic(), partition()) -> ok.
stop_consuming(Topic, Partition) ->
  kafkerl_connector:stop_fetch(Topic, Partition).

%% Metadata API
-spec request_metadata() -> ok.
request_metadata() ->
  request_metadata([]).

-spec request_metadata([topic()]) -> ok.
request_metadata(Topics) when is_list(Topics) ->
  kafkerl_connector:request_metadata(Topics).

%% Partitions
-spec partitions() -> [{topic(), [partition()]}] | error().
partitions() ->
  kafkerl_connector:get_partitions().

%% Utils
-spec version() -> {integer(), integer(), integer()}.
version() ->
  {3, 0, 0}.