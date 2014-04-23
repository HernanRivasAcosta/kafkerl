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
  ConnStart = {kafkerl_connector, start_link, [get_connector_name(),
                                               get_producer_conn_config()]},
  ProducerStart = {kafkerl_producer, start_link, [get_producer_options()]},
  {ok, {{one_for_one, 5, 10},
        [{kafkerl_connector, ConnStart, permanent, 2000, worker,
          [kafkerl_connector]},
         {kafkerl_producer, ProducerStart, permanent, 2000, worker,
          [kafkerl_producer]}]}}.

get_producer_options() ->
  case application:get_env(kafkerl, producer_options) of
    undefined ->
      lager:error("unable to load producer options"),
      [];
    {ok, Config} ->
      lists:keymerge(1, Config, [{connector, get_connector_name()}])
  end.

get_producer_conn_config() ->
  case application:get_env(kafkerl, producer_conn_config) of
    undefined ->
      lager:error("unable to load producer connection config"),
      [];
    {ok, ConnConfig} ->
      ConnConfig
  end.

get_connector_name() ->
  kafkerl_producer_connector.