-module(kafkerl_utils).
-author('hernanrivasacosta@gmail.com').

-export([callback/2, callback/3, error/2]).
-export([get_tcp_options/1]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

%%==============================================================================
%% API
%%==============================================================================
callback(_Callback, []) ->
  ok;
callback(Callback, Data) ->
  do_callback(kafka_message, Callback, Data).
callback(Callback, Metadata, Data) ->
  do_callback(kafka_message, Callback, Metadata, Data).

error(Callback, Data) ->
  do_callback(kafka_error, Callback, Data).

do_callback(_Type, {M, F}, Data) ->
  spawn(fun() -> M:F(Data) end),
  ok;
do_callback(_Type, {M, F, A}, Data) ->
  spawn(fun() -> apply(M, F, A ++ [Data]) end),
  ok;
do_callback(Type, Pid, Data) when is_pid(Pid) ->
  Pid ! {Type, Data},
  ok;
do_callback(_Type, Function, Data) when is_function(Function, 1) ->
  Function(Data),
  ok.

do_callback(_Type, {M, F}, Metadata, Data) ->
  spawn(fun() -> M:F(Metadata, Data) end),
  ok;
do_callback(_Type, {M, F, A}, Metadata, Data) ->
  spawn(fun() -> apply(M, F, A ++ [Metadata, Data]) end),
  ok;
do_callback(Type, Pid, Metadata, Data) when is_pid(Pid) ->
  Pid ! {Type, Metadata, Data},
  ok;
do_callback(_Type, Function, Metadata, Data) when is_function(Function, 2) ->
  Function(Metadata, Data),
  ok.

get_tcp_options(Options) -> % TODO: refactor
  lists:ukeymerge(1, lists:sort(proplists:unfold(Options)), ?DEFAULT_TCP_OPTS).