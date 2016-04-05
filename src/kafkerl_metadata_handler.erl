-module(kafkerl_metadata_handler).
-author('hernanrivasacosta@gmail.com').

-behaviour(gen_fsm).

%% API
-export([request_metadata/2]).
-export([idle/2, requesting/2, on_cooldown/2]).
% gen_fsm
-export([start_link/1, init/1]).

-record(state, {max_metadata_retries = -1 :: integer(),
                retry_interval        = 1 :: non_neg_integer(),
                metadata_request_cd   = 0 :: integer()}).

%%==============================================================================
%% API
%%==============================================================================
-spec start_link(atom(), any()) -> {ok, pid()} | ignore | kafkerl:error().
start_link(Name, Config) ->
  gen_fsm:start_link({local, Name}, ?MODULE, [Config], []).

-spec request_metadata(atom(), [topic()]) -> ok.
request_metadata(ServerRef, Topics) ->
  ok.

%%==============================================================================
%% States
%%==============================================================================
idle(_, State) ->
  {next_state, open, {[], Code}, 30000};.

requesting(_, State) ->
  ok.

on_cooldown(_, State) ->
  ok.

%%==============================================================================
%% Handlers
%%==============================================================================
init([Config]) ->
  Schema = [{metadata_tcp_timeout, positive_integer, {default, 1500}},
            {metadata_request_cooldown, positive_integer, {default, 333}},
            {max_metadata_retries, {integer, {-1, undefined}}, {default, -1}}],
  case normalizerl:normalize_proplist(Schema, Config) of
    {ok, [RetryInterval, MetadataRequestCD, MaxMetadataRetries]} ->
      State = #state{config               = Config,
                     retry_interval       = RetryInterval,
                     metadata_request_cd  = MetadataRequestCD,
                     max_metadata_retries = MaxMetadataRetries},
      {ok, State};
    {errors, Errors} ->
      ok = lists:foreach(fun(E) ->
                           _ = lager:critical("Metadata config error ~p", [E])
                         end, Errors),
      {stop, bad_config}
  end.