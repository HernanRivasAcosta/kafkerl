-module(kafkerl).
-author('hernanrivasacosta@gmail.com').

-export([start/0, start/2]).
-export([version/0,
         produce/1, produce/2, produce_messages_from_file/1,
         produce_messages_from_file/2, produce_messages_from_file/3,
         get_partitions/0, get_partitions/1,
         subscribe/1, subscribe/2, subscribe/3,
         unsubscribe/1, unsubscribe/2,
         request_metadata/0, request_metadata/1, request_metadata/2,
         valid_message/1]).

-include("kafkerl.hrl").
-include("kafkerl_consumers.hrl").

%% Types
-type produce_option()  :: {buffer_size, integer() | infinity} | 
                           {dump_location, string()}.
-type produce_options() :: [produce_option()].

-export_type([produce_options/0]).

%%==============================================================================
%% API
%%==============================================================================
start() ->
  application:load(?MODULE),
  application:start(?MODULE).

start(_StartType, _StartArgs) ->
  kafkerl_sup:start_link().

%%==============================================================================
%% Access API
%%==============================================================================
-spec version() -> {integer(), integer(), integer()}.
version() ->
  {1, 1, 1}.

-spec produce(basic_message()) -> ok.
produce(Message) ->
  produce(?MODULE, Message).
-spec produce(atom(), basic_message()) -> ok;
             (basic_message(), produce_options()) -> ok.
produce(Message, Options) when is_tuple(Message) ->
  produce(?MODULE, Message, Options);
produce(Name, Message) ->
  produce(Name, Message, []).
-spec produce(atom(), basic_message(), produce_options()) -> ok.
produce(Name, Message, Options) ->
  kafkerl_connector:send(Name, Message, Options).

-spec produce_messages_from_file(string()) -> ok.
produce_messages_from_file(Filename) ->
  produce_messages_from_file(?MODULE, Filename).
-spec produce_messages_from_file(atom(), basic_message()) -> ok;
                                (string(), produce_options()) -> ok.
produce_messages_from_file(Filename, Options) when is_list(Filename) ->
  produce_messages_from_file(?MODULE, Filename, Options);
produce_messages_from_file(Name, Filename) ->
  produce_messages_from_file(Name, Filename, []).
-spec produce_messages_from_file(atom(), string(), produce_options()) -> ok.
produce_messages_from_file(Name, Filename, Options) ->
  {ok, Bin} = file:read_file(Filename),
  Messages = binary_to_term(Bin),
  [produce(Name, M, Options) || M <- Messages],
  ok.

-spec get_partitions() -> [{topic(), [partition()]}] | error().
get_partitions() ->
  get_partitions(?MODULE).
-spec get_partitions(atom()) -> [{topic(), [partition()]}] | error().
get_partitions(Name) ->
  kafkerl_connector:get_partitions(Name).

-spec subscribe(callback()) -> ok.
subscribe(Callback) ->
  subscribe(?MODULE, Callback).
-spec subscribe(atom(), callback()) -> ok.
subscribe(Callback, all = Filter) ->
  subscribe(?MODULE, Callback, Filter);
subscribe(Callback, Filter) when is_list(Filter) ->
  subscribe(?MODULE, Callback, Filter);
subscribe(Name, Callback) ->
  subscribe(Name, Callback, all).
-spec subscribe(atom(), callback(), filters()) -> ok.
subscribe(Name, Callback, Filter) ->
  kafkerl_connector:subscribe(Name, Callback, Filter).

-spec unsubscribe(callback()) -> ok.
unsubscribe(Callback) ->
  unsubscribe(?MODULE, Callback).
-spec unsubscribe(atom(), callback()) -> ok.
unsubscribe(Name, Callback) ->
  kafkerl_connector:unsubscribe(Name, Callback).

-spec request_metadata() -> ok.
request_metadata() ->
  request_metadata(?MODULE).
-spec request_metadata(atom() | [topic()]) -> ok.
request_metadata(Name) when is_atom(Name) ->
  kafkerl_connector:request_metadata(Name);
request_metadata(Topics) ->
  request_metadata(?MODULE, Topics).
-spec request_metadata(atom(), [topic()]) -> ok.
request_metadata(Name, Topics) ->
  kafkerl_connector:request_metadata(Name, Topics).

-spec valid_message(any()) -> boolean().
valid_message(Any) ->
  kafkerl_utils:valid_message(Any).