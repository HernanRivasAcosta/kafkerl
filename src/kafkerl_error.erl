-module(kafkerl_error).
-author('hernanrivasacosta@gmail.com').

-export([get_error_name/1, get_error_description/1, get_error_tuple/1]).


get_error_name(0) ->
  "NoError";
get_error_name(1) ->
  "OffsetOutOfRange";
get_error_name(2) ->
  "InvalidMessage";
get_error_name(3) ->
  "UnknownTopicOrPartition";
get_error_name(4) ->
  "InvalidMessageSize";
get_error_name(5) ->
  "LeaderNotAvailable";
get_error_name(6) ->
  "NotLeaderForPartition";
get_error_name(7) ->
  "RequestTimedOut";
get_error_name(8) ->
  "BrokerNotAvailable";
get_error_name(9) ->
  "ReplicaNotAvailable";
get_error_name(10) ->
  "MessageSizeTooLarge";
get_error_name(11) ->
  "StaleControllerEpoch";
get_error_name(12) ->
  "OffsetMetadataTooLarge";
get_error_name(14) ->
  "OffsetsLoadInProgressCode";
get_error_name(15) ->
  "ConsumerCoordinatorNotAvailableCode";
get_error_name(16) ->
  "NotCoordinatorForConsumerCode";
get_error_name(-1) ->
  "Unknown".

get_error_description(0) ->
  "No error";
get_error_description(1) ->
  "The requested offset is outside the range of offsets maintained by the " ++
  "server for the given topic/partition.";
get_error_description(2) ->
  "If you specify a string larger than configured maximum for offset metadata.";
get_error_description(3) ->
  "This request is for a topic or partition that does not exist on this broker";
get_error_description(4) ->
  "The message has a negative size.";
get_error_description(5) ->
  "This error is thrown if we are in the middle of a leadership election " ++
  "and there is currently no leader for this partition and hence it is " ++
  "unavailable for writes.";
get_error_description(6) ->
  "This error is thrown if the client attempts to send messages to a " ++
  "replica that is not the leader for some partition. It indicates that the " ++
  "clients metadata is out of date.";
get_error_description(7) ->
  "This error is thrown if the request exceeds the user-specified time " ++
  "limit in the request.";
get_error_description(8) ->
  "This is not a client facing error and is used only internally by " ++
  "intra-cluster broker communication.";
get_error_description(9) ->
  "Unused.";
get_error_description(10) ->
  "The server has a configurable maximum message size to avoid unbounded " ++
  "memory allocation. This error is thrown if the client attempt to produce " ++
  "a message larger than this maximum.";
get_error_description(11) ->
  "Internal error code for broker-to-broker communication.";
get_error_description(12) ->
  "If you specify a string larger than configured maximum for offset metadata.";
get_error_description(14) ->
  "The broker returns this error code for an offset fetch request if it is " ++
  "still loading offsets (after a leader change for that offsets topic " ++
  "partition).";
get_error_description(15) ->
  "The broker returns this error code for consumer metadata requests or " ++
  "offset commit requests if the offsets topic has not yet been created.";
get_error_description(16) ->
  "The broker returns this error code if it receives an offset fetch or " ++
  "commit request for a consumer group that it is not a coordinator for.";
get_error_description(-1) ->
  "An unexpected server error".

get_error_tuple(0) ->
  {error, no_error};
get_error_tuple(1) ->
  {error, offset_out_of_range};
get_error_tuple(2) ->
  {error, invalid_message};
get_error_tuple(3) ->
  {error, unknown_topic_or_partition};
get_error_tuple(4) ->
  {error, invalid_message_size};
get_error_tuple(5) ->
  {error, leader_not_available};
get_error_tuple(6) ->
  {error, not_leader_for_partition};
get_error_tuple(7) ->
  {error, request_timedout};
get_error_tuple(8) ->
  {error, broker_not_available};
get_error_tuple(9) ->
  {error, replica_not_available};
get_error_tuple(10) ->
  {error, message_size_too_large};
get_error_tuple(11) ->
  {error, stale_controller_epoch};
get_error_tuple(12) ->
  {error, offset_metadata_too_large};
get_error_tuple(14) ->
  {error, offsets_load_in_progress_code};
get_error_tuple(15) ->
  {error, consumer_coordinator_not_available_code};
get_error_tuple(16) ->
  {error, not_coordinator_for_consumer_code};
get_error_tuple(-1) ->
  {error, unknown}.