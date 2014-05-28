-module(kafkerl_utils).
-author('hernanrivasacosta@gmail.com').

-export([callback/2, callback/3, error/2]).

-include("kafkerl_consumers.hrl").

%%==============================================================================
%% API
%%==============================================================================
callback(Callback, []) ->
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
do_callback(Type, Pid, Data) ->
  Pid ! {Type, Data},
  ok.

do_callback(_Type, {M, F}, Metadata, Data) ->
  spawn(fun() -> M:F(Metadata, Data) end),
  ok;
do_callback(_Type, {M, F, A}, Metadata, Data) ->
  spawn(fun() -> apply(M, F, A ++ [Metadata, Data]) end),
  ok;
do_callback(Type, Pid, Metadata, Data) ->
  Pid ! {Type, Metadata, Data},
  ok.