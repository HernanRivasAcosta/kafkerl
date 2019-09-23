
-module(kafkerl_buffer).
-author("anders").
-behavior(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% API
-export([start_link/0, start_link/1, create_buffer/1, create_buffer/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(_) ->
    start_link().


init([]) ->
    {ok, []}.

create_buffer(Name, Type) ->
    gen_server:call(?MODULE, {create_buffer, Name, Type}).
create_buffer(Name) ->
    gen_server:call(?MODULE, {create_buffer, Name}).

handle_call({create_buffer, Name, Type}, _from, State) ->
    Alredy_Exists = ets_buffer:list(Name) =/= [],
    Res = ets_buffer:create(Name, Type),
    lager:debug("buffer ~p type ~p created ~p, already exists ~p", [Name, Type, Res, Alredy_Exists]),
    {reply, ok, State};
handle_call({create_buffer, Name}, _From, State) ->
    Res = ets_buffer:create(Name),
    lager:debug("buffer ~p created ~p", [Name, Res]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
