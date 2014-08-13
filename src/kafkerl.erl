-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0, start/2]).
-export([produce/1, produce/2, get_partitions/0, get_partitions/1]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

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
-spec produce(basic_message()) -> ok.
produce(Message) ->
  produce(?MODULE, Message).
-spec produce(atom(), basic_message()) -> ok.
produce(Name, Message) ->
  kafkerl_connector:send(Name, Message).

-spec get_partitions() -> [{topic(), [partition()]}] | error().
get_partitions() ->
  get_partitions(?MODULE).
-spec get_partitions(atom()) -> [{topic(), [partition()]}] | error().
get_partitions(Name) ->
  kafkerl_connector:get_partitions(Name).