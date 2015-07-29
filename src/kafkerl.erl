-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0, start/2]).
-export([produce/3, produce/4, produce/5,
         consume/2, consume/3, consume/4,
         request_metadata/0, request_metadata/1, request_metadata/2,
         partitions/0, partitions/1]).
-export([version/0]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

%% Types
-type option()     :: {buffer_size, integer() | infinity} | 
                      {dump_location, string()} | 
                      {consumer, callback()} |
                      {min_bytes, integer()} |
                      {max_wait, integer()} |
                      {offset, integer()}.
-type options()    :: [option()].
-type server_ref() :: atom() | pid().

-export_type([options/0, server_ref/0]).

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
-spec produce(topic(), partition(), payload()) -> ok.
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

-spec consume(topic(), partition(), options()) -> ok | [binary()] | error();
             (server_ref(), topic(), partition()) -> ok | error().
consume(Topic, Partition, Options) when is_list(Options) ->
  consume(?MODULE, Topic, Partition, Options);
consume(ServerRef, Topic, Partition) ->
  consume(ServerRef, Topic, Partition, []).

-spec consume(server_ref(), topic(), partition(), options()) ->
  ok | [binary()] | error().
consume(ServerRef, Topic, Partition, Options) ->
  case lists:keyfind(consumer, 1, Options) of
    false ->
      NewOptions = [{consumer, self()} | Options],
      kafkerl_connector:fetch(ServerRef, Topic, Partition, NewOptions),
      kafkerl_utils:gather_consume_responses();
    _ ->
      kafkerl_connector:fetch(ServerRef, Topic, Partition, Options)
  end.

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