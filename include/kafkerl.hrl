-define(KAFKERL_COMPRESSION_NONE,   none).
-define(KAFKERL_COMPRESSION_GZIP,   gzip).
-define(KAFKERL_COMPRESSION_SNAPPY, snappy).

-type kafkerl_compression() :: ?KAFKERL_COMPRESSION_NONE |
                               ?KAFKERL_COMPRESSION_GZIP |
                               ?KAFKERL_COMPRESSION_SNAPPY.

-define(KAFKERL_COMPRESSION_TYPES, [?KAFKERL_COMPRESSION_NONE,
                                    ?KAFKERL_COMPRESSION_GZIP,
                                    ?KAFKERL_COMPRESSION_SNAPPY]).

-type kafkerl_topic()       :: binary().
-type kafkerl_partition()   :: integer().
-type kafkerl_payload()     :: binary() | [binary()].
-type kafkerl_message()     :: {kafkerl_topic(), kafkerl_partition(),
                                kafkerl_payload()} |
                               {kafkerl_topic(), [{kafkerl_partition(),
                                                   kafkerl_payload()}]} |
                               [kafkerl_message()].

-type kafkerl_conn_config() :: [{host, string()} |
                                {port, integer()} |
                                {tcp_options, any()} |
                                {max_retries, integer(),
                                {compression, kafkerl_compression()}}].