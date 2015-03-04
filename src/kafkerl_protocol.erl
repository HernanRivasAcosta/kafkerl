-module(kafkerl_protocol).
-author('hernanrivasacosta@gmail.com').

-export([build_produce_request/4, build_fetch_request/5,
         build_metadata_request/3]).

-export([parse_produce_response/1, parse_fetch_response/1,
         parse_fetch_response/2, parse_metadata_response/1]).

-include("kafkerl.hrl").

%%==============================================================================
%% API
%%==============================================================================
% Message building
-spec build_produce_request(merged_message(), client_id(), correlation_id(),
                            compression()) -> iodata().
build_produce_request(Data, ClientId, CorrelationId, Compression) ->
  {Size, Request} = build_produce_request(Data, Compression),
  [build_request_header(ClientId, ?PRODUCE_KEY, CorrelationId, Size), Request].

-spec build_fetch_request(fetch_request(), client_id(), correlation_id(),
                          integer(), integer()) -> iodata().
build_fetch_request(Data, ClientId, CorrelationId, MaxWait, MinBytes) ->
  {Size, Request} = build_fetch_request(Data, MaxWait, MinBytes),
  [build_request_header(ClientId, ?FETCH_KEY, CorrelationId, Size), Request].

-spec build_metadata_request(topic() | [topic()], correlation_id(),
                             client_id()) -> iodata().
build_metadata_request(Topics, CorrelationId, ClientId) ->
  {_Size, Request} = build_metadata_request(Topics),
  [build_request_header(ClientId, ?METADATA_KEY, CorrelationId), Request].

% Message parsing
-spec parse_produce_response(binary()) -> produce_response().
parse_produce_response(<<_Size:32/unsigned-integer,
                         CorrelationId:32/unsigned-integer,
                         TopicCount:32/unsigned-integer,
                         TopicsBin/binary>>) ->
  {ok, Topics} = parse_produced_topics(TopicCount, TopicsBin),
  {ok, CorrelationId, Topics}.

-spec parse_fetch_response(binary()) -> fetch_response().
parse_fetch_response(<<_Size:32/unsigned-integer,
                       CorrelationId:32/unsigned-integer,
                       TopicCount:32/unsigned-integer,
                       TopicsBin/binary>>) ->
  case parse_topics(TopicCount, TopicsBin) of
    {ok, Topics} ->
      {ok, CorrelationId, Topics};
    {incomplete, Topics, {Bin, Steps}} ->
      {incomplete, CorrelationId, Topics, {Bin, CorrelationId, Steps}}
  end;
parse_fetch_response(_Other) ->
  {error, unexpected_binary}.

-spec parse_fetch_response(binary(), fetch_state()) -> fetch_response().
parse_fetch_response(Bin, {Remainder, CorrelationId, Steps}) ->
  NewBin = <<Remainder/binary, Bin/binary>>,
  parse_steps(NewBin, CorrelationId, Steps).

-spec parse_metadata_response(binary()) -> metadata_response().
parse_metadata_response(<<CorrelationId:32/unsigned-integer,
                          BrokerCount:32/unsigned-integer,
                          BrokersBin/binary>>) ->
  case parse_brokers(BrokerCount, BrokersBin) of
    {ok, Brokers, <<TopicCount:32/unsigned-integer, TopicsBin/binary>>} ->
      case parse_topic_metadata(TopicCount, TopicsBin) of
        {ok, Metadata} ->
          {ok, CorrelationId, {Brokers, Metadata}};
        Error ->
          Error
      end;
    Error ->
      Error
  end;
parse_metadata_response(_Other) ->
  {error, unexpected_binary}.

%%==============================================================================
%% Message Building
%%==============================================================================
build_request_header(ClientId, ApiKey, CorrelationId) ->
  % Build the header (http://goo.gl/5SNNTV)
  ApiVersion = 0, % Both the key and version should be 0, it's not a placeholder
  ClientIdSize = byte_size(ClientId),
  [<<ApiKey:16/unsigned-integer,
     ApiVersion:16/unsigned-integer,
     CorrelationId:32/unsigned-integer,
     ClientIdSize:16/unsigned-integer>>,
   ClientId].

build_request_header(ClientId, ApiKey, CorrelationId, RequestSize) ->
  ClientIdSize = byte_size(ClientId),
  % 10 is the size of the header
  MessageSize = ClientIdSize + RequestSize + 10,
  [<<MessageSize:32/unsigned-integer>>,
   build_request_header(ClientId, ApiKey, CorrelationId),
   ClientId].

%% PRODUCE REQUEST
build_produce_request([{Topic, Partition, Messages}], Compression) ->
  build_produce_request({Topic, Partition, Messages}, Compression);
build_produce_request([{Topic, [{Partition, Messages}]}], Compression) ->
  build_produce_request({Topic, Partition, Messages}, Compression);
build_produce_request({Topic, [{Partition, Messages}]}, Compression) ->
  build_produce_request({Topic, Partition, Messages}, Compression);
build_produce_request({Topic, Partition, Messages}, Compression) ->
  % This is a fast version used when producing for a single topic and partition
  TopicSize = byte_size(Topic),
  {Size, MessageSet} = build_message_set(Messages, Compression),
  {Size + TopicSize + 24,
   [<<-1:16/signed-integer,
      -1:32/signed-integer, % Timeout
      1:32/unsigned-integer,  % TopicCount
      TopicSize:16/unsigned-integer>>,
    Topic,
    <<1:32/unsigned-integer,  % PartitionCount
      Partition:32/unsigned-integer,
      Size:32/unsigned-integer>>,
    MessageSet]};
build_produce_request(Data, Compression) ->
  % Build the body of the request with multiple topics/partitions
  % (Docs at: http://goo.gl/J3C50c)
  TopicCount = length(Data),
  {TopicsSize, Topics} = build_topics(Data, Compression),
  % 10 is the size of the header
  {TopicsSize + 10,
   [<<-1:16/signed-integer, % RequiredAcks
      -1:32/signed-integer, % Timeout
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
  {Size + TopicSize + 6,
   [<<TopicSize:16/unsigned-integer,
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
  {Size + 8,
   [<<Partition:32/unsigned-integer,
      Size:32/unsigned-integer>>,
    MessageSet]}.

% Docs at http://goo.gl/4W7J0r
build_message_set(Message, _Compression) when is_binary(Message) ->
  build_message(Message);
build_message_set(Messages, Compression) ->
  build_message_set(Messages, Compression, {0, []}).

build_message_set([] = _Messages, ?COMPRESSION_NONE, {Size, IOList}) ->
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
  Message = [get_message_header(BinSize, ?COMPRESSION_NONE), Bin],
  Offset = 0, % This number is completely irrelevant when sent from the producer
  Size = BinSize + 14, % 14 is the size of the header plus the Crc
  Crc = erlang:crc32(Message),
  % 12 is the size of the offset plus the size int itself
  {Size + 12,
   [<<Offset:64/unsigned-integer,
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

compression_to_int(?COMPRESSION_NONE)   -> 0;
compression_to_int(?COMPRESSION_GZIP)   -> 1;
compression_to_int(?COMPRESSION_SNAPPY) -> 2.

compress(?COMPRESSION_NONE,   Data) -> Data;
compress(?COMPRESSION_GZIP,   Data) -> Data;
compress(?COMPRESSION_SNAPPY, Data) -> Data.

%% FETCH REQUEST
build_fetch_request([{Topic, {Partition, Offset, MaxBytes}}],
                    MaxWait, MinBytes) ->
  build_fetch_request({Topic, {Partition, Offset, MaxBytes}},
                      MaxWait, MinBytes);
build_fetch_request([{Topic, [{Partition, Offset, MaxBytes}]}],
                    MaxWait, MinBytes) ->
  build_fetch_request({Topic, {Partition, Offset, MaxBytes}},
                      MaxWait, MinBytes);
build_fetch_request({Topic, {Partition, Offset, MaxBytes}},
                    MaxWait, MinBytes) ->
  TopicSize = byte_size(Topic),
  {TopicSize + 38,
   [<<-1:32/signed-integer,  % ReplicaId
      MaxWait:32/unsigned-integer,
      MinBytes:32/unsigned-integer,
      1:32/unsigned-integer, % TopicCount
      TopicSize:16/unsigned-integer>>,
    Topic,
    <<1:32/unsigned-integer, % PartitionCount
      Partition:32/unsigned-integer,
      Offset:64/unsigned-integer,
      MaxBytes:32/unsigned-integer>>]};
build_fetch_request(Data, MaxWait, MinBytes) ->
  ReplicaId = -1, % This should always be -1
  TopicCount = length(Data),
  {TopicSize, Topics} = build_fetch_topics(Data),
  % 16 is the size of the header
  {TopicSize + 16,
   [<<ReplicaId:32/signed-integer,
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
  {Size + TopicSize + 6,
   [<<TopicSize:16/unsigned-integer,
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
  {16,
   <<Partition:32/unsigned-integer,
     Offset:64/unsigned-integer,
     MaxBytes:32/unsigned-integer>>}.

build_metadata_request([]) ->
  % Builds an empty metadata request that returns all topics and partitions
  {4, <<0:32/unsigned-integer>>};
build_metadata_request(Topic) when is_binary(Topic) ->
  build_metadata_request([Topic]);
build_metadata_request(Topics) ->
  TopicCount = length(Topics),
  {Size, BuiltTopics} = build_metadata_topics(Topics),
  {Size + 4,
   [<<TopicCount:32/unsigned-integer>>,
    BuiltTopics]}.

build_metadata_topics(Topics) ->
  build_metadata_topics(Topics, {0, []}).

build_metadata_topics([] = _Topics, {Size, IOList}) ->
  {Size, lists:reverse(IOList)};
build_metadata_topics([H | T] = _Partitions, {OldSize, IOList}) ->
  Size = byte_size(H),
  Topic = [<<Size:16/unsigned-integer>>, H],
  build_metadata_topics(T, {OldSize + Size + 2, [Topic | IOList]}).

%%==============================================================================
%% Message Parsing
%%==============================================================================
% Parse produce response (http://goo.gl/f7zhbg)
parse_produced_topics(Count, Bin) ->
  parse_produced_topics(Count, Bin, []).

parse_produced_topics(Count, <<>>, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc)};
parse_produced_topics(Count, Bin, Acc) when Count =< 0 ->
  lager:warning("Finished parsing produce response, ignoring bytes: ~p", [Bin]),
  {ok, lists:reverse(Acc)};
parse_produced_topics(Count, <<TopicNameLength:16/unsigned-integer,
                               TopicName:TopicNameLength/binary,
                               PartitionCount:32/unsigned-integer,
                               PartitionsBin/binary>>, Acc) ->
  {ok, Partitions, Remainder} = parse_produced_partitions(PartitionCount,
                                                          PartitionsBin),
  parse_produced_topics(Count - 1, Remainder, [{TopicName, Partitions} | Acc]).

parse_produced_partitions(Count, Bin) ->
  parse_produced_partitions(Count, Bin, []).

parse_produced_partitions(Count, Bin, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc), Bin};
parse_produced_partitions(Count, <<Partition:32/unsigned-integer,
                                   ErrorCode:16/signed-integer,
                                   Offset:64/unsigned-integer,
                                   Remainder/binary>>, Acc) ->
  PartitionData = {Partition, ErrorCode, Offset},
  parse_produced_partitions(Count - 1, Remainder, [PartitionData | Acc]).


% Parse fetch response (http://goo.gl/eba5z3)
parse_topics(Count, Bin) ->
  parse_topics(Count, Bin, []).

parse_topics(Count, <<>>, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc)};
parse_topics(Count, Bin, Acc) when Count =< 0 ->
  lager:warning("Finished parsing topics, ignoring bytes: ~p", [Bin]),
  {ok, lists:reverse(Acc)};
parse_topics(Count, Bin, Acc) ->
  case parse_topic(Bin) of
    {ok, Topic, Remainder} ->
      parse_topics(Count - 1, Remainder, [Topic | Acc]);
    {incomplete, Topic, {Remainder, Steps}} ->
      Step = {topics, Count},
      {incomplete, lists:reverse(Acc, [Topic]), {Remainder, Steps ++ [Step]}};
    incomplete ->
      {incomplete, lists:reverse(Acc), {Bin, [{topics, Count}]}}
  end.

parse_topic(<<TopicNameLength:16/unsigned-integer,
              TopicName:TopicNameLength/binary,
              PartitionCount:32/unsigned-integer,
              PartitionsBin/binary>>) ->
  case parse_partitions(PartitionCount, PartitionsBin) of
    {ok, Partitions, Remainder} ->
      {ok, {TopicName, Partitions}, Remainder};
    {incomplete, Partitions, {Bin, Steps}} ->
      Step = {topic, TopicName},
      {incomplete, {TopicName, Partitions}, {Bin, Steps ++ [Step]}}
  end;
parse_topic(_Bin) ->
  incomplete.

parse_partitions(Count, Bin) ->
  parse_partitions(Count, Bin, []).

parse_partitions(Count, Bin, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc), Bin};
parse_partitions(Count, Bin, Acc) ->
  case parse_partition(Bin) of
    {ok, Partition, Remainder} ->
      parse_partitions(Count - 1, Remainder, [Partition | Acc]);
    {incomplete, Partition, {Remainder, Steps}} ->
      Step = {partitions, Count},
      NewState = {Remainder, Steps ++ [Step]},
      {incomplete, lists:reverse(Acc, [Partition]), NewState};
    incomplete ->
      Step = {partitions, Count},
      {incomplete, lists:reverse(Acc), {Bin, [Step]}}
  end.

parse_partition(<<PartitionId:32/unsigned-integer,
                  0:16/signed-integer,
                  HighwaterMarkOffset:64/unsigned-integer,
                  MessageSetSize:32/unsigned-integer,
                  MessageSetBin/binary>>) ->
  Partition = {PartitionId, HighwaterMarkOffset},
  case parse_message_set(MessageSetSize, MessageSetBin) of
    {ok, Messages, Remainder} ->
      {ok, {Partition, Messages}, Remainder};
    {incomplete, Messages, {Bin, Steps}} ->
      Step = {partition, Partition},
      {incomplete, {Partition, Messages}, {Bin, Steps ++ [Step]}}
  end;
parse_partition(<<_Partition:32/unsigned-integer,
                  ErrorCode:16/signed-integer,
                  _/binary>>) ->
  kafkerl_error:get_error_tuple(ErrorCode);
parse_partition(<<>>) ->
  incomplete.

parse_message_set(Size, Bin) ->
  parse_message_set(Size, Bin, []).

parse_message_set(Count, Bin, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc), Bin};
parse_message_set(RemainingSize, Bin, Acc) ->
  case parse_message(Bin) of
    {ok, {Message, Size}, Remainder} ->
      parse_message_set(RemainingSize - Size, Remainder, [Message | Acc]);
    incomplete ->
      {incomplete, lists:reverse(Acc), {Bin, [{message_set, RemainingSize}]}}
  end.

parse_message(<<_Offset:64/unsigned-integer,
                MessageSize:32/signed-integer,
                Message:MessageSize/binary,
                Remainder/binary>>) ->
  <<_Crc:32/unsigned-integer,
    _MagicByte:8/unsigned-integer,
    _Attributes:8/unsigned-integer,
    KeyValue/binary>> = Message,
  KV = case KeyValue of
         <<KeySize:32/unsigned-integer, Key:KeySize/binary,
           ValueSize:32/unsigned-integer, Value:ValueSize/binary>> ->
             {Key, Value};
         % 4294967295 is -1 and it signifies an empty Key http://goo.gl/Ssl4wq
         <<4294967295:32/unsigned-integer,
           ValueSize:32/unsigned-integer, Value:ValueSize/binary>> ->
             {no_key, Value}
       end,
  % 12 is the size of the offset plus the size of the MessageSize int
  {ok, {KV, MessageSize + 12}, Remainder};
parse_message(_) ->
  incomplete.

% Parse metadata response (http://goo.gl/3wxlZt)
parse_brokers(Count, Bin) ->
  parse_brokers(Count, Bin, []).

parse_brokers(Count, Bin, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc), Bin};
parse_brokers(Count, <<Id:32/unsigned-integer,
                       HostLength:16/unsigned-integer,
                       Host:HostLength/binary,
                       Port:32/unsigned-integer,
                       Remainder/binary>>, Acc) ->
  HostStr = binary_to_list(Host),
  parse_brokers(Count - 1, Remainder, [{Id, {HostStr, Port}} | Acc]).

parse_topic_metadata(Count, Bin) ->
  parse_topic_metadata(Count, Bin, []).

parse_topic_metadata(Count, <<>>, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc)};
parse_topic_metadata(Count, Bin, Acc) when Count =< 0 ->
  lager:warning("Finished parsing topic metadata, ignoring bytes: ~p", [Bin]),
  {ok, lists:reverse(Acc)};
parse_topic_metadata(Count, <<0:16/signed-integer,
                              TopicSize:16/unsigned-integer,
                              TopicName:TopicSize/binary,
                              PartitionCount:32/unsigned-integer,
                              PartitionsBin/binary>>, Acc) ->
  {ok, PartitionsMetadata, Remainder} = parse_partition_metadata(PartitionCount,
                                                                 PartitionsBin),
  TopicMetadata = {0, TopicName, PartitionsMetadata},
  parse_topic_metadata(Count - 1, Remainder, [TopicMetadata | Acc]);
parse_topic_metadata(Count, <<ErrorCode:16/signed-integer,
                              -1:16/signed-integer, % TopicSize
                              0:32/unsigned-integer, % PartitionCount
                              Remainder/binary>>, Acc) ->
  {ok, PartitionsMetadata, Remainder} = parse_partition_metadata(0, Remainder),
  TopicMetadata = {ErrorCode, <<"unkown">>, PartitionsMetadata},
  parse_topic_metadata(Count - 1, Remainder, [TopicMetadata | Acc]).
  
parse_partition_metadata(Count, Bin) ->
  parse_partition_metadata(Count, Bin, []).

parse_partition_metadata(Count, Remainder, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc), Remainder};
parse_partition_metadata(Count, <<ErrorCode:16/unsigned-integer,
                                  Partition:32/unsigned-integer,
                                  Leader:32/signed-integer,
                                  ReplicaCount:32/unsigned-integer,
                                  ReplicasBin/binary>>, Acc) ->
  {ok, Replicas, Remainder} = parse_replica_metadata(ReplicaCount, ReplicasBin),
  <<IsrCount:32/unsigned-integer, IsrBin/binary>> = Remainder,
  {ok, Isr, IsrRemainder} = parse_isr_metadata(IsrCount, IsrBin),
  PartitionMetadata = {ErrorCode, Partition, Leader, Replicas, Isr},
  parse_partition_metadata(Count - 1, IsrRemainder, [PartitionMetadata | Acc]).

parse_replica_metadata(Count, Bin) ->
  parse_replica_metadata(Count, Bin, []).

parse_replica_metadata(Count, Remainder, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc), Remainder};
parse_replica_metadata(Count, <<Replica:32/unsigned-integer,
                                 Remainder/binary>>, Acc) ->
  parse_replica_metadata(Count - 1, Remainder, [Replica | Acc]).

parse_isr_metadata(Count, Bin) ->
  parse_isr_metadata(Count, Bin, []).

parse_isr_metadata(Count, Remainder, Acc) when Count =< 0 ->
  {ok, lists:reverse(Acc), Remainder};
parse_isr_metadata(Count, <<Isr:32/unsigned-integer,
                            Remainder/binary>>, Acc) ->
  parse_isr_metadata(Count - 1, Remainder, [Isr | Acc]).

%%==============================================================================
%% Utils (aka: don't repeat code)
%%==============================================================================
parse_steps(Bin, CorrelationId, Steps) ->
  parse_steps(Bin, CorrelationId, Steps, void).

parse_steps(<<>>, CorrelationId, [], Data) ->
  {ok, CorrelationId, Data};
parse_steps(Bin, CorrelationId, [Step | T], Data) ->
  case parse_step(Bin, Step, Data) of
    {ok, NewData} ->
      {ok, CorrelationId, NewData};
    {ok, NewData, NewBin} ->
      parse_steps(NewBin, CorrelationId, T, NewData);
    {incomplete, NewData, {NewBin, Steps}} ->
      NewState = {NewBin, CorrelationId, Steps ++ T},
      DataWithContext = add_context_to_data(NewData, Steps ++ T),
      {incomplete, CorrelationId, DataWithContext, NewState};
    {incomplete, Steps} ->
      NewState = {Bin, CorrelationId, Steps ++ T},
      DataWithContext = add_context_to_data(Data, Steps ++ T),
      {incomplete, CorrelationId, DataWithContext, NewState};
    {add_steps, NewBin, NewData, Steps} ->
      parse_steps(NewBin, CorrelationId, Steps ++ T, NewData);
    Error = {error, _Reason} ->
      Error
  end.

parse_step(Bin, {topic, void}, Topics) ->
  case parse_topic(Bin) of
    {incomplete, Topic, {Remainder, Steps}} ->
      {add_steps, Remainder, lists:reverse(Topics, [Topic]), Steps};
    incomplete ->
      {incomplete, []};
    {ok, Topic, Remainder} ->
      {ok, [Topic | Topics], Remainder}
  end;
parse_step(Bin, {topic, TopicName},
           [{Partition, Partitions} | Topics]) when is_integer(Partition) ->
  {ok, [{TopicName, [{Partition, Partitions}]} | Topics], Bin};
parse_step(<<>>, {topic, TopicName}, Data) ->
  {ok, [{TopicName, Data}]};
parse_step(_Bin, {topic, TopicName}, _Data) ->
  {incomplete, [{topic, TopicName}]};

parse_step(Bin, {topics, Count}, void) ->
  parse_topics(Count, Bin);
parse_step(Bin, {topics, 1}, Topics) ->
  {ok, Topics, Bin};
parse_step(Bin, {topics, Count}, Topics) ->
  {add_steps, Bin, Topics, [{topic, void}, {topics, Count - 1}]};

parse_step(Bin, {partition, Partition}, Messages) ->
  {ok, [{Partition, Messages}], Bin};

parse_step(Bin, {partitions, Count}, void) ->
  parse_partitions(Count, Bin);
parse_step(Bin, {partitions, 1}, Partitions) ->
  {ok, Partitions, Bin};

parse_step(Bin, {message_set, RemainingSize}, _) ->
  case parse_message_set(RemainingSize, Bin) of
    {incomplete, Messages, State} ->
      {incomplete, Messages, State};
    {ok, Messages, <<>>} ->
      {ok, Messages, <<>>};
    {ok, Messages, Remainder} ->
      {ok, Messages, Remainder}
  end.

add_context_to_data(Data, []) ->
  Data;
add_context_to_data(Data, [{partition, Partition} | T]) ->
  add_context_to_data([{Partition, Data}], T);
add_context_to_data(Data, [{topic, Topic} | T]) ->
  add_context_to_data([{Topic, Data}], T);
add_context_to_data(Data, [_H | T]) ->
  add_context_to_data(Data, T).