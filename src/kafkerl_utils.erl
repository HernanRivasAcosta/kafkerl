-module(kafkerl_utils).
-author('hernanrivasacosta@gmail.com').

-export([send_event/3, send_error/2]).
-export([get_tcp_options/1]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

%%==============================================================================
%% API
%%==============================================================================
-spec send_error(callback(), any()) -> ok.
send_error(Target, Reason) ->
  send_event(error, Target, Reason).

-spec send_event(atom(), callback(), any()) -> ok | {error, bad_callback}.
send_event(_Type, {M, F}, Data) ->
  spawn(fun() -> M:F(Data) end),
  ok;
send_event(_Type, {M, F, A}, Data) ->
  spawn(fun() -> apply(M, F, A ++ [Data]) end),
  ok;
send_event(Type, Pid, Data) when is_pid(Pid) ->
  Pid ! {Type, Data},
  ok;
send_event(_Type, Function, Data) when is_function(Function, 1) ->
  spawn(fun() -> Function(Data) end),
  ok;
send_event(_, _, _) ->
  {error, bad_callback}.

get_tcp_options(Options) -> % TODO: refactor
  lists:ukeymerge(1, lists:sort(proplists:unfold(Options)), ?DEFAULT_TCP_OPTS).