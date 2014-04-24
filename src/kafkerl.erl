-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0, start/2]).

start() ->
  application:load(kafkerl),
  application:start(kafkerl).

start(_StartType, _StartArgs) ->
  {Producers, _Consumers} = get_services_to_start(),
  kafkerl_producer_sup:start_link(Producers).

%%==============================================================================
%% Utils
%%==============================================================================
get_services_to_start() ->
  case application:get_env(kafkerl, start) of
    undefined  -> {[], []};
    {ok, List} -> Producers = [Config || {producer, Config} <- List],
                  Consumers = [Config || {consumer, Config} <- List],
                  {Producers, Consumers}
  end.