%% Constants
% Misc
-define(ETS_BUFFER, ets_buffer).
-define(DEFAULT_TCP_OPTS, lists:sort([{mode, binary}, {packet, 0}])).
% Compression
-define(COMPRESSION_NONE,   none).
-define(COMPRESSION_GZIP,   gzip).
-define(COMPRESSION_SNAPPY, snappy).
-define(KAFKERL_COMPRESSION_TYPES, [?COMPRESSION_NONE,
                                    ?COMPRESSION_GZIP,
                                    ?COMPRESSION_SNAPPY]).
% API keys
-define(PRODUCE_KEY,  0).
-define(FETCH_KEY,    1).
-define(OFFSET_KEY,   2).
-define(METADATA_KEY, 3).

%% Common
-type error_code()     :: -1..16.
-type correlation_id() :: non_neg_integer().

%% Connection
-type address_host()   :: string().
-type address_port()   :: 1..65535.
-type socket_address() :: {address_host(), address_port()}.
-type broker_id()      :: integer().
-type broker()         :: {broker_id(), socket_address()}.

%% Configuration
-type compression() :: ?COMPRESSION_NONE |
                       ?COMPRESSION_GZIP |
                       ?COMPRESSION_SNAPPY.

%% Requests
-type client_id()      :: binary().
-type topic()          :: binary().
-type partition()      :: integer().
-type payload()        :: binary() | [binary()].
-type basic_message()  :: {topic(), partition(), payload()}.
-type merged_message() :: basic_message() |
                          {topic(), [{partition(), payload()}]} |
                          [merged_message()].

-type fetch_offset()    :: integer().
-type fetch_max_bytes() :: integer().
-type fetch_partition() :: {partition(), fetch_offset(), fetch_max_bytes()} |
                           [fetch_partition()].
-type fetch_request()   :: {topic(), fetch_partition()} |
                           [fetch_request()].

%% Reponses
-type error() :: {error, atom() | {atom(), any()}}.

%% Produce responses
-type produce_partition() :: {partition(), error_code(), integer()}.
-type produce_topic()     :: {topic(), [produce_partition()]}.
-type produce_response()  :: {ok, correlation_id(), [produce_topic()]}.

%% Fetch responses
-type messages()       :: [{topic(), [{{partition(), integer()}, [binary()]}]}].
-type fetch_state()    :: {binary(), integer(), [any()]}.
-type fetch_response() :: {ok, integer(), messages()} |
                          {incomplete, integer(), messages(), fetch_state()} |
                          error().

%% Metadata responses
-type leader()             :: integer().
-type replica()            :: integer().
-type isr()                :: integer().
-type partition_metadata() :: {error_code(), partition(), broker_id(),
                               [replica()], [isr()]}.
-type topic_metadata()     :: {error_code(), topic(), [partition_metadata()]}.
-type metadata()           :: {[broker()], [topic_metadata()]}.
-type metadata_response()  :: {ok, correlation_id(), metadata()} |
                              error().

%% Error codes
-define(NO_ERROR, 0).
-define(OFFSET_OUT_OF_RANGE, 1).
-define(INVALID_MESSAGE, 2).
-define(UNKNOWN_TOPIC_OR_PARTITION, 3).
-define(INVALID_MESSAGE_SIZE, 4).
-define(LEADER_NOT_AVAILABLE, 5).
-define(NOT_LEADER_FOR_PARTITION, 6).
-define(REQUEST_TIMEDOUT, 7).
-define(BROKER_NOT_AVAILABLE, 8).
-define(REPLICA_NOT_AVAILABLE, 9).
-define(MESSAGE_SIZE_TOO_LARGE, 10).
-define(STALE_CONTROLLER_EPOCH, 11).
-define(OFFSET_METADATA_TOO_LARGE, 12).
-define(OFFSETS_LOAD_IN_PROGRESS_CODE, 14).
-define(CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE, 15).
-define(NOT_COORDINATOR_FOR_CONSUMER_CODE, 16).
-define(UNKNOWN, -1).
