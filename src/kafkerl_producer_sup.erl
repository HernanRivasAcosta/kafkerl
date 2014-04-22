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
  ProducerStart = {kafkerl_producer, start_link, [get_producer_conn_config(),
                                                  get_producer_options()]},
  {ok, {{one_for_one, 5, 10},
        [{kafkerl_producer, ProducerStart, permanent, 2000, worker,
          [kafkerl_producer]}]}}.

get_producer_options() ->
  case application:get_env(kafkerl, producer_options) of
    undefined ->
      lager:error("unable to load producer options"),
      [];
    {ok, Config} ->
     Config
  end.

get_producer_conn_config() ->
  case application:get_env(kafkerl, producer_conn_config) of
    undefined ->
      lager:error("unable to load producer connection config"),
      [];
    {ok, ConnConfig} ->
      ConnConfig
  end.