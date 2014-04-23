-module(kafkerl_protocol).
-author('hernanrivasacosta@gmail.com').

-export([build_produce_request/4, build_fetch_request/5]).

-include("kafkerl.hrl").

%%==============================================================================
%% API
%%==============================================================================
% Message building
-spec build_produce_request(kafkerl_message(), binary(), integer(),
                             kafkerl_compression()) -> iodata().
build_produce_request(Data, ClientId, CorrelationId, Compression) ->
  {Size, Request} = build_produce_request(Data, Compression),
  [build_request_header(ClientId, 0, CorrelationId, Size), Request].

-spec build_fetch_request(kafkerl_fetch_request(), binary(), integer(),
                          integer(), integer()) -> iodata().
build_fetch_request(Data, ClientId, CorrelationId, MaxWait, MinBytes) ->
  {Size, Request} = build_fetch_request(Data, MaxWait, MinBytes),
  [build_request_header(ClientId, 1, CorrelationId, Size), Request].

% Message parsing
-type fetch_state()      :: {integer(), {integer(), kafkerl_topic()},
                            {integer(), kafkerl_partition()}, binary()}.
-type fetched_messages() :: [{kafkerl_topic(),
                              [{kafkerl_partition(), [binary()]}]}].
-type fetch_response()   :: {ok, integer(), fetched_messages()} |
                            {incomplete, integer(), fetched_messages(),
                             fetch_state()} |
                            {error, bad_format}.

-spec parse_fetch_response(binary()) -> fetch_response().
parse_fetch_response(<<CorrelationId:32/unsigned-integer,
                       FetchResponse/binary>>) ->
  <<TopicCount:32/unsigned-integer, TopicsBin/binary>> = FetchResponse,
  case parse_topics(TopicCount, TopicsBin) of
    {ok, Topics} ->
      {ok, CorrelationId, Topics};
    {incomplete, Topics, {LastTopic, LastPartition, Remainder}} ->
      {incomplete, CorrelationId, Topics, {LastTopic, LastPartition, Remainder}}
  end.

-spec parse_fetch_response(fetch_state(), binary()) -> fetch_response().
parse_fetch_response(State, Bin) ->
  ok.

%%==============================================================================
%% Utils
%%==============================================================================
build_request_header(ClientId, ApiKey, CorrelationId, RequestSize) ->
  % Build the header (http://goo.gl/5SNNTV)
  ApiVersion = 0, % Both the key and version should be 0, it's not a placeholder
  ClientIdSize = byte_size(ClientId),
  % 10 is the size of the header
  MessageSize = ClientIdSize + RequestSize + 10,
  <<MessageSize:32/unsigned-integer,
     ApiKey:16/unsigned-integer,
     ApiVersion:16/unsigned-integer,
     CorrelationId:32/unsigned-integer,
     ClientIdSize:16/unsigned-integer,
     ClientId/binary>>.

%% PRODUCE REQUEST
build_produce_request(SimpleData, Compression) when is_tuple(SimpleData) ->
  build_produce_request([SimpleData], Compression);
build_produce_request(Data, Compression) ->
  % Build the body of the request (Docs at: http://goo.gl/J3C50c)
  RequiredAcks = 0,
  Timeout = -1,
  TopicCount = length(Data),
  {TopicsSize, Topics} = build_topics(Data, Compression),
  % 10 is the size of the header
  {TopicsSize + 10, [<<RequiredAcks:16/unsigned-integer,
                       Timeout:32/unsigned-integer,
                       TopicCount:32/unsigned-integer>>,
                     Topics]}.

build_topics(Topics, Compression) ->
  build_topics(Topics, Compression, {0, []}).

build_topics([] = _Topics, _Compression, {Size, IOList}) ->
  {Size, lists:reverse(IOList)};
build_topics([H | T] = _Topics, Compression, {OldSize, IOList}) ->
  {Size, Topic} = build_topic(H, Compression),
  build_topics(T, Compression, {OldSize + Size, [Topic | IOList]}).

build_topic({Topic, Partition, Value}, Compression) ->
  build_topic({Topic, [{Partition, Value}]}, Compression);
build_topic({Topic, Partitions}, Compression) ->
  TopicSize = byte_size(Topic),
  PartitionCount = length(Partitions),
  {Size, BuiltPartitions} = build_partitions(Partitions, Compression),
  % 6 is the size of both the partition count int and the topic size int
  {Size + TopicSize + 6, [<<TopicSize:16/unsigned-integer,
                            Topic/binary,
                            PartitionCount:32/unsigned-integer>>,
                          BuiltPartitions]}.

build_partitions(Partitions, Compression) ->
  build_partitions(Partitions, Compression, {0, []}).

build_partitions([] = _Partitions, _Compression, {Size, IOList}) ->
  {Size, lists:reverse(IOList)};
build_partitions([H | T] = _Partitions, Compression, {OldSize, IOList}) ->
  {Size, Partition} = build_partition(H, Compression),
  build_partitions(T, Compression, {OldSize + Size, [Partition | IOList]}).

build_partition({Partition, Message}, Compression) when is_binary(Message) ->
  build_partition({Partition, [Message]}, Compression);
build_partition({Partition, Messages}, Compression) ->
  {Size, MessageSet} = build_message_set(Messages, Compression),
  % 8 is the size of the header, 4 bytes of the partition and 4 for the size
  {Size + 8, [<<Partition:32/unsigned-integer, Size:32/unsigned-integer>>,
              MessageSet]}.

% Docs at http://goo.gl/4W7J0r
build_message_set(Messages, Compression) ->
  build_message_set(Messages, Compression, {0, []}).

build_message_set([] = _Messages, ?KAFKERL_COMPRESSION_NONE, {Size, IOList}) ->
  {Size, lists:reverse(IOList)};
build_message_set([] = _Messages, Compression, {_Size, IOList}) ->
  Compressed = compress(Compression, lists:reverse(IOList)),
  CompressedSize = iolist_size(Compressed),
  Header = get_message_header(CompressedSize, Compression),
  {byte_size(Header) + CompressedSize, [Header, Compressed]};
build_message_set([H | T] = _Messages, Compression, {OldSize, IOList}) ->
  {Size, Message} = build_message(H),
  build_message_set(T, Compression, {OldSize + Size, [Message | IOList]}).

build_message(Bin) ->
  % Docs at: http://goo.gl/xWrdPF
  BinSize = byte_size(Bin),
  Message = [get_message_header(BinSize, ?KAFKERL_COMPRESSION_NONE), Bin],
  Offset = 0, % This number is completely irrelevant when sent from the producer
  Size = BinSize + 14, % 14 is the size of the header plus the Crc
  Crc = erlang:crc32(Message),
  % 12 is the size of the offset plus the size int itself
  {Size + 12, [<<Offset:64/unsigned-integer,
                 Size:32/unsigned-integer,
                 Crc:32/unsigned-integer>>,
               Message]}.

get_message_header(MessageSize, Compression) ->
  MagicByte = 0, % Version id
  Attributes = compression_to_int(Compression),
  <<MagicByte:8/unsigned-integer,
    Attributes:8/unsigned-integer,
    -1:32/signed-integer,
    MessageSize:32/unsigned-integer>>.

compression_to_int(?KAFKERL_COMPRESSION_NONE)   -> 0;
compression_to_int(?KAFKERL_COMPRESSION_GZIP)   -> 1;
compression_to_int(?KAFKERL_COMPRESSION_SNAPPY) -> 2;
% Error handling
compression_to_int(Other) -> lager:error("invalid compression ~p", [Other]), 0.

compress(?KAFKERL_COMPRESSION_NONE,   Data) -> Data;
compress(?KAFKERL_COMPRESSION_GZIP,   Data) -> Data;
compress(?KAFKERL_COMPRESSION_SNAPPY, Data) -> Data.

%% FETCH REQUEST
build_fetch_request(SimpleData, MaxWait, MinBytes) when is_tuple(SimpleData) ->
  build_fetch_request([SimpleData], MaxWait, MinBytes);
build_fetch_request(Data, MaxWait, MinBytes) ->
  ReplicaId = -1, % This should always be -1
  TopicCount = length(Data),
  {TopicSize, Topics} = build_fetch_topics(Data),
  % 16 is the size of the header
  {TopicSize + 16, [<<ReplicaId:32/signed-integer,
                      MaxWait:32/unsigned-integer,
                      MinBytes:32/unsigned-integer,
                      TopicCount:32/unsigned-integer>>,
                    Topics]}.

build_fetch_topics(Topics) ->
  build_fetch_topics(Topics, {0, []}).

build_fetch_topics([] = _Topics, {Size, IOList}) ->
  {Size, lists:reverse(IOList)};
build_fetch_topics([H | T] = _Topics, {OldSize, IOList}) ->
  {Size, Topic} = build_fetch_topic(H),
  build_fetch_topics(T, {OldSize + Size, [Topic | IOList]}).

build_fetch_topic({Topic, Partition}) when is_tuple(Partition) ->
  build_fetch_topic({Topic, [Partition]});
build_fetch_topic({Topic, Partitions}) ->
  TopicSize = byte_size(Topic),
  PartitionCount = length(Partitions),
  {Size, BuiltPartitions} = build_fetch_partitions(Partitions),
  % 6 is the size of the topicSize's 16 bytes + 32 from the partition count
  {Size + TopicSize + 6, [<<TopicSize:16/unsigned-integer,
                            Topic/binary,
                            PartitionCount:32/unsigned-integer>>,
                          BuiltPartitions]}.

build_fetch_partitions(Partitions) ->
  build_fetch_partitions(Partitions, {0, []}).

build_fetch_partitions([] = _Partitions, {Size, IOList}) ->
  {Size, lists:reverse(IOList)};
build_fetch_partitions([H | T] = _Partitions, {OldSize, IOList}) ->
  {Size, Partition} = build_fetch_partition(H),
  build_fetch_partitions(T, {OldSize + Size, [Partition | IOList]}).

build_fetch_partition({Partition, Offset, MaxBytes}) ->
  {16, <<Partition:32/unsigned-integer,
         Offset:64/unsigned-integer,
         MaxBytes:32/unsigned-integer>>}.

% PARSE FETCH RESPONSE
parse_topics(Count, Bin) ->
  parse_topics(Count, Bin, []).

parse_topics(0, Bin, Acc) ->
  lager:info("parsed all topics, remainder is ~p", [Bin]),
  {ok, lists:reverse(Acc)};
parse_topics(Count, Bin, Acc) ->
  case parse_topic(Bin) of
    {ok, Topic, Remainder} ->
      parse_topics(Count - 1, Remainder, [Topic | Acc]);
    {incomplete, {TopicName, LastPartition, Remainder}} ->
      CurrentTopic = {Count, TopicName},
      {incomplete, lists:reverse(Acc), {CurrentTopic, LastPartition, Remainder}}
  end.

parse_topic(<<TopicNameLength:16/unsigned-integer,
              TopicName:TopicNameLength/binary,
              PartitionCount:32/unsigned-integer,
              PartitionBin>>) ->
  ok.