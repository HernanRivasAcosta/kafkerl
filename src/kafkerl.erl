-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0, start/2]).
%% Ease of access API
-export([produce/1, produce/2]).

-include("kafkerl.hrl").

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
produce(Module, Message) ->
  kafkerl_connector:send(Module, Message).