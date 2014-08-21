-module(kafkerl_sup).
-author('hernanrivasacosta@gmail.com').

-behaviour(supervisor).

-export([start_link/0, init/1]).

-include("kafkerl.hrl").

-define(SERVER, ?MODULE).

-type restart_strategy() :: {supervisor:strategy(),
                             non_neg_integer(),
                             non_neg_integer()}.

%%==============================================================================
%% API
%%==============================================================================
-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%==============================================================================
%% Utils
%%==============================================================================
-spec init([]) -> {ok, {restart_strategy(), [supervisor:child_spec()]}}.
init([]) ->
  {ok, {{one_for_one, 5, 10}, [get_connector_child_spec(),
                               get_buffer_child_spec()]}}.

get_connector_child_spec() ->
  Name = case application:get_env(kafkerl, gen_server_name) of
           {ok, Value} -> Value;
           _           -> kafkerl
         end,
  {ok, ConnConfig} = application:get_env(kafkerl, conn_config),
  Topics = case application:get_env(kafkerl, topics) of
             {ok, Any} -> Any;
             undefined -> []
           end,
  Params = [Name, [{topics, Topics} | ConnConfig]],
  MFA = {kafkerl_connector, start_link, Params},
  {Name, MFA, permanent, 2000, worker, [kafkerl_connector]}.

get_buffer_child_spec() ->
  {ok, BufferConfig} = application:get_env(kafkerl, buffer),

  MFA = {kafkerl_buffer, start_link, [BufferConfig]},
  {?ETS_BUFFER, MFA, permanent, 2000, worker, [kafkerl_buffer]}.