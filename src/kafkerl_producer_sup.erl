-module(kafkerl_producer_sup).
-author('hernan@inakanetworks.net').

-behaviour(supervisor).

-export([start_link/0, init/1]).

-define(SERVER, ?MODULE).

-type producer_config() :: any().
-type consumer_config() :: any().

%%==============================================================================
%% API
%%==============================================================================
-spec start_link() -> {ok, pid()}.
start_link() ->
  Services = get_services_to_start(),
  supervisor:start_link({local, ?SERVER}, ?MODULE, Services).

%%==============================================================================
%% Utils
%%==============================================================================
-type restart_strategy() :: {supervisor:strategy(), integer(), integer()}.
-spec init({[producer_config()], [consumer_config()]}) ->
  {ok, {restart_strategy(), [supervisor:child_spec()]}}.
init({ProducerConfigs, _ConsumerConfigs}) ->
  ProdConnectors = [get_connector_child_spec(X) || X <- ProducerConfigs],
  Producers = [get_producer_child_spec(X) || X <- ProducerConfigs],
  {ok, {{one_for_one, 5, 10}, ProdConnectors ++ Producers}}.

get_services_to_start() ->
  case application:get_env(kafkerl, start) of
    undefined  -> {[], []};
    {ok, List} -> Producers = [Config || {producer, Config} <- List],
                  Consumers = [Config || {consumer, Config} <- List],
                  {Producers, Consumers}
  end.

get_connector_child_spec(ProducerConfig) ->
  {name, Preffix} = lists:keyfind(name, 1, ProducerConfig),
  Name = get_connector_name(Preffix),
  {conn_config, ConnConfig} = lists:keyfind(conn_config, 1, ProducerConfig),
  MFA = {kafkerl_connector, start_link, [Name, ConnConfig]},
  {Name, MFA, permanent, 2000, worker, [kafkerl_connector]}.

get_producer_child_spec(ProducerConfig) ->
  {name, Name} = lists:keyfind(name, 1, ProducerConfig),
  {config, Config} = lists:keyfind(config, 1, ProducerConfig),
  Config2 = Config ++ [{connector, get_connector_name(Name)}],
  MFA = {kafkerl_producer, start_link, [Name, Config2]},
  {Name, MFA, permanent, 2000, worker, [kafkerl_producer]}.

get_connector_name(ProducerName) ->
  list_to_atom(atom_to_list(ProducerName) ++ "_connector").