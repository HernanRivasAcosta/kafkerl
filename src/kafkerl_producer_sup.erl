-module(kafkerl_producer_sup).
-author('hernan@inakanetworks.net').

-behaviour(supervisor).

-export([start_link/0, init/1]).


-define(SERVER, ?MODULE).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%==============================================================================
%% Utils
%%==============================================================================
-type restart_strategy() :: {supervisor:strategy(), integer(), integer()}.
-spec init([]) -> {ok, {restart_strategy(), [supervisor:child_spec()]}}.
init([]) ->
  Config = case application:get_env(kafkerl, producer) of
             undefined  -> lager:error("unable to load producer config"),
                           [];
             {ok, List} -> List
           end,
  {ok, {{one_for_one, 5, 10},
        [{kafkerl_producer, {kafkerl_producer, start_link, [Config]},
         permanent, 2000, worker, [kafkerl_producer]}]}}.