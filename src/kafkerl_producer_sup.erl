-module(kafkerl_producer_sup).
-author('hernan@inakanetworks.net').

-behaviour(supervisor).

-export([start_link/1, init/1]).


-define(SERVER, ?MODULE).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link([any()]) -> {ok, pid()}.
start_link(ProducerConfigs) ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, [ProducerConfigs]).

%%==============================================================================
%% Utils
%%==============================================================================
-type restart_strategy() :: {supervisor:strategy(), integer(), integer()}.
-spec init([]) -> {ok, {restart_strategy(), [supervisor:child_spec()]}}.
init([ProducerConfigs]) ->
  GetConnector = fun(L) ->
                   {name, Preffix} = lists:keyfind(name, 1, L),
                   Name = get_connector_name(Preffix),
                   {conn_config, Config} = lists:keyfind(conn_config, 1, L),
                   MFA = {kafkerl_connector, start_link, [Name, Config]},
                   {Name, MFA, permanent, 2000, worker, [kafkerl_connector]}
                 end,
  GetProducer = fun(L) ->
                  {name, Name} = lists:keyfind(name, 1, L),
                  {config, Config} = lists:keyfind(config, 1, L),
                  Config2 = Config ++ [{connector, get_connector_name(Name)}],
                  MFA = {kafkerl_producer, start_link, [Name, Config2]},
                  {Name, MFA, permanent, 2000, worker, [kafkerl_producer]}
                end,

  Connectors = lists:map(GetConnector, ProducerConfigs),
  Producers = lists:map(GetProducer, ProducerConfigs),
  {ok, {{one_for_one, 5, 10}, Connectors ++ Producers}}.

get_connector_name(ProducerName) ->
  list_to_atom(atom_to_list(ProducerName) ++ "_connector").