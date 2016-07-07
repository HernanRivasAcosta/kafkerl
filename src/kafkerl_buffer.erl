
-module(kafkerl_buffer).
-author("anders").
-behavior(gen_server).

%% API
-export([start_link/0, init/1, create_buffer/2, handle_call/3, start_link/1, create_buffer/1]).

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


