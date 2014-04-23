-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0, start/2]).

start() ->
  application:load(kafkerl),
  application:start(kafkerl).

start(_StartType, _StartArgs) ->
  kafkerl_producer_sup:start_link(),
  kafkerl_consumer_sup:start_link().