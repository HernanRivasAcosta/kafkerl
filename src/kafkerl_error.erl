-module(kafkerl_error).
-author('hernanrivasacosta@gmail.com').

-export([get_error_name/1, get_error_description/1, get_error_tuple/1]).

-include("kafkerl.hrl").

%%==============================================================================
%% API
%%==============================================================================
get_error_name(?NO_ERROR) ->
  "NoError";
get_error_name(?OFFSET_OUT_OF_RANGE) ->
  "OffsetOutOfRange";
get_error_name(?INVALID_MESSAGE) ->
  "InvalidMessage";
get_error_name(?UNKNOWN_TOPIC_OR_PARTITION) ->
  "UnknownTopicOrPartition";
get_error_name(?INVALID_MESSAGE_SIZE) ->
  "InvalidMessageSize";
get_error_name(?LEADER_NOT_AVAILABLE) ->
  "LeaderNotAvailable";
get_error_name(?NOT_LEADER_FOR_PARTITION) ->
  "NotLeaderForPartition";
get_error_name(?REQUEST_TIMEDOUT) ->
  "RequestTimedOut";
get_error_name(?BROKER_NOT_AVAILABLE) ->
  "BrokerNotAvailable";
get_error_name(?REPLICA_NOT_AVAILABLE) ->
  "ReplicaNotAvailable";
get_error_name(?MESSAGE_SIZE_TOO_LARGE) ->
  "MessageSizeTooLarge";
get_error_name(?STALE_CONTROLLER_EPOCH) ->
  "StaleControllerEpoch";
get_error_name(?OFFSET_METADATA_TOO_LARGE) ->
  "OffsetMetadataTooLarge";
get_error_name(?OFFSETS_LOAD_IN_PROGRESS_CODE) ->
  "OffsetsLoadInProgressCode";
get_error_name(?CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE) ->
  "ConsumerCoordinatorNotAvailableCode";
get_error_name(?NOT_COORDINATOR_FOR_CONSUMER_CODE) ->
  "NotCoordinatorForConsumerCode";
get_error_name(?UNKNOWN) ->
  "Unknown".

get_error_description(?NO_ERROR) ->
  "No error";
get_error_description(?OFFSET_OUT_OF_RANGE) ->
  "The requested offset is outside the range of offsets maintained by the " ++
  "server for the given topic/partition.";
get_error_description(?INVALID_MESSAGE) ->
  "If you specify a string larger than configured maximum for offset metadata.";
get_error_description(?UNKNOWN_TOPIC_OR_PARTITION) ->
  "This request is for a topic or partition that does not exist on this broker";
get_error_description(?INVALID_MESSAGE_SIZE) ->
  "The message has a negative size.";
get_error_description(?LEADER_NOT_AVAILABLE) ->
  "This error is thrown if we are in the middle of a leadership election " ++
  "and there is currently no leader for this partition and hence it is " ++
  "unavailable for writes.";
get_error_description(?NOT_LEADER_FOR_PARTITION) ->
  "This error is thrown if the client attempts to send messages to a " ++
  "replica that is not the leader for some partition. It indicates that the " ++
  "clients metadata is out of date.";
get_error_description(?REQUEST_TIMEDOUT) ->
  "This error is thrown if the request exceeds the user-specified time " ++
  "limit in the request.";
get_error_description(?BROKER_NOT_AVAILABLE) ->
  "This is not a client facing error and is used only internally by " ++
  "intra-cluster broker communication.";
get_error_description(?REPLICA_NOT_AVAILABLE) ->
  "Unused.";
get_error_description(?MESSAGE_SIZE_TOO_LARGE) ->
  "The server has a configurable maximum message size to avoid unbounded " ++
  "memory allocation. This error is thrown if the client attempt to produce " ++
    "a message larger than this maximum.";
get_error_description(?STALE_CONTROLLER_EPOCH) ->
  "Internal error code for broker-to-broker communication.";
get_error_description(?OFFSET_METADATA_TOO_LARGE) ->
  "If you specify a string larger than configured maximum for offset metadata.";
get_error_description(?OFFSETS_LOAD_IN_PROGRESS_CODE) ->
  "The broker returns this error code for an offset fetch request if it is " ++
  "still loading offsets (after a leader change for that offsets topic " ++
  "partition).";
get_error_description(?CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE) ->
  "The broker returns this error code for consumer metadata requests or " ++
  "offset commit requests if the offsets topic has not yet been created.";
get_error_description(?NOT_COORDINATOR_FOR_CONSUMER_CODE) ->
  "The broker returns this error code if it receives an offset fetch or " ++
  "commit request for a consumer group that it is not a coordinator for.";
get_error_description(?UNKNOWN) ->
  "An unexpected server error".

get_error_tuple(?NO_ERROR) ->
  {error, no_error};
get_error_tuple(?OFFSET_OUT_OF_RANGE) ->
  {error, offset_out_of_range};
get_error_tuple(?INVALID_MESSAGE) ->
  {error, invalid_message};
get_error_tuple(?UNKNOWN_TOPIC_OR_PARTITION) ->
  {error, unknown_topic_or_partition};
get_error_tuple(?INVALID_MESSAGE_SIZE) ->
  {error, invalid_message_size};
get_error_tuple(?LEADER_NOT_AVAILABLE) ->
  {error, leader_not_available};
get_error_tuple(?NOT_LEADER_FOR_PARTITION) ->
  {error, not_leader_for_partition};
get_error_tuple(?REQUEST_TIMEDOUT) ->
  {error, request_timedout};
get_error_tuple(?BROKER_NOT_AVAILABLE) ->
  {error, broker_not_available};
get_error_tuple(?REPLICA_NOT_AVAILABLE) ->
  {error, replica_not_available};
get_error_tuple(?MESSAGE_SIZE_TOO_LARGE) ->
  {error, message_size_too_large};
get_error_tuple(?STALE_CONTROLLER_EPOCH) ->
  {error, stale_controller_epoch};
get_error_tuple(?OFFSET_METADATA_TOO_LARGE) ->
  {error, offset_metadata_too_large};
get_error_tuple(?OFFSETS_LOAD_IN_PROGRESS_CODE) ->
  {error, offsets_load_in_progress_code};
get_error_tuple(?CONSUMER_COORDINATOR_NOT_AVAILABLE_CODE) ->
  {error, consumer_coordinator_not_available_code};
get_error_tuple(?NOT_COORDINATOR_FOR_CONSUMER_CODE) ->
  {error, not_coordinator_for_consumer_code};
get_error_tuple(?UNKNOWN) ->
  {error, unknown}.