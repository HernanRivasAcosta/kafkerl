-module(kafkerl_error).
-author('hernanrivasacosta@gmail.com').

-export([get_error_name/1, get_error_description/1, get_error_tuple/1]).

-include("kafkerl.hrl").

%%==============================================================================
%% API
%%==============================================================================
get_error_name(?UNKNOWN_SERVER_ERROR) ->
    "UnknownServerError";
get_error_name(?NONE) ->
    "None";
get_error_name(?OFFSET_OUT_OF_RANGE) ->
    "OffsetOutOfRange";
get_error_name(?CORRUPT_MESSAGE) ->
    "CorruptMessage";
get_error_name(?UNKNOWN_TOPIC_OR_PARTITION) ->
    "UnknownTopicOrPartition";
get_error_name(?INVALID_FETCH_SIZE) ->
    "InvalidFetchSize";
get_error_name(?LEADER_NOT_AVAILABLE) ->
    "LeaderNotAvailable";
get_error_name(?NOT_LEADER_OR_FOLLOWER) ->
    "NotLeaderOrFollower";
get_error_name(?REQUEST_TIMED_OUT) ->
    "RequestTimedOut";
get_error_name(?BROKER_NOT_AVAILABLE) ->
    "BrokerNotAvailable";
get_error_name(?REPLICA_NOT_AVAILABLE) ->
    "ReplicaNotAvailable";
get_error_name(?MESSAGE_TOO_LARGE) ->
    "MessageTooLarge";
get_error_name(?STALE_CONTROLLER_EPOCH) ->
    "StaleControllerEpoch";
get_error_name(?OFFSET_METADATA_TOO_LARGE) ->
    "OffsetMetadataTooLarge";
get_error_name(?NETWORK_EXCEPTION) ->
    "NetworkException";
get_error_name(?COORDINATOR_LOAD_IN_PROGRESS) ->
    "CoordinatorLoadInProgress";
get_error_name(?COORDINATOR_NOT_AVAILABLE) ->
    "CoordinatorNotAvailable";
get_error_name(?NOT_COORDINATOR) ->
    "NotCoordinator";
get_error_name(?INVALID_TOPIC_EXCEPTION) ->
    "InvalidTopicException";
get_error_name(?RECORD_LIST_TOO_LARGE) ->
    "RecordListTooLarge";
get_error_name(?NOT_ENOUGH_REPLICAS) ->
    "NotEnoughReplicas";
get_error_name(?NOT_ENOUGH_REPLICAS_AFTER_APPEND) ->
    "NotEnoughReplicasAfterAppend";
get_error_name(?INVALID_REQUIRED_ACKS) ->
    "InvalidRequiredAcks";
get_error_name(?ILLEGAL_GENERATION) ->
    "IllegalGeneration";
get_error_name(?INCONSISTENT_GROUP_PROTOCOL) ->
    "InconsistentGroupProtocol";
get_error_name(?INVALID_GROUP_ID) ->
    "InvalidGroupId";
get_error_name(?UNKNOWN_MEMBER_ID) ->
    "UnknownMemberId";
get_error_name(?INVALID_SESSION_TIMEOUT) ->
    "InvalidSessionTimeout";
get_error_name(?REBALANCE_IN_PROGRESS) ->
    "RebalanceInProgress";
get_error_name(?INVALID_COMMIT_OFFSET_SIZE) ->
    "InvalidCommitOffsetSize";
get_error_name(?TOPIC_AUTHORIZATION_FAILED) ->
    "TopicAuthorizationFailed";
get_error_name(?GROUP_AUTHORIZATION_FAILED) ->
    "GroupAuthorizationFailed";
get_error_name(?CLUSTER_AUTHORIZATION_FAILED) ->
    "ClusterAuthorizationFailed";
get_error_name(?INVALID_TIMESTAMP) ->
    "InvalidTimestamp";
get_error_name(?UNSUPPORTED_SASL_MECHANISM) ->
    "UnsupportedSaslMechanism";
get_error_name(?ILLEGAL_SASL_STATE) ->
    "IllegalSaslState";
get_error_name(?UNSUPPORTED_VERSION) ->
    "UnsupportedVersion";
get_error_name(?TOPIC_ALREADY_EXISTS) ->
    "TopicAlreadyExists";
get_error_name(?INVALID_PARTITIONS) ->
    "InvalidPartitions";
get_error_name(?INVALID_REPLICATION_FACTOR) ->
    "InvalidReplicationFactor";
get_error_name(?INVALID_REPLICA_ASSIGNMENT) ->
    "InvalidReplicaAssignment";
get_error_name(?INVALID_CONFIG) ->
    "InvalidConfig";
get_error_name(?NOT_CONTROLLER) ->
    "NotController";
get_error_name(?INVALID_REQUEST) ->
    "InvalidRequest";
get_error_name(?UNSUPPORTED_FOR_MESSAGE_FORMAT) ->
    "UnsupportedForMessageFormat";
get_error_name(?POLICY_VIOLATION) ->
    "PolicyViolation";
get_error_name(?OUT_OF_ORDER_SEQUENCE_NUMBER) ->
    "OutOfOrderSequenceNumber";
get_error_name(?DUPLICATE_SEQUENCE_NUMBER) ->
    "DuplicateSequenceNumber";
get_error_name(?INVALID_PRODUCER_EPOCH) ->
    "InvalidProducerEpoch";
get_error_name(?INVALID_TXN_STATE) ->
    "InvalidTxnState";
get_error_name(?INVALID_PRODUCER_ID_MAPPING) ->
    "InvalidProducerIdMapping";
get_error_name(?INVALID_TRANSACTION_TIMEOUT) ->
    "InvalidTransactionTimeout";
get_error_name(?CONCURRENT_TRANSACTIONS) ->
    "ConcurrentTransactions";
get_error_name(?TRANSACTION_COORDINATOR_FENCED) ->
    "TransactionCoordinatorFenced";
get_error_name(?TRANSACTIONAL_ID_AUTHORIZATION_FAILED) ->
    "TransactionalIdAuthorizationFailed";
get_error_name(?SECURITY_DISABLED) ->
    "SecurityDisabled";
get_error_name(?OPERATION_NOT_ATTEMPTED) ->
    "OperationNotAttempted";
get_error_name(?KAFKA_STORAGE_ERROR) ->
    "KafkaSotrageError";
get_error_name(?LOG_DIR_NOT_FOUND) ->
    "LogDirNotFound";
get_error_name(?SASL_AUTHENTICATION_FAILED) ->
    "SaslAuthenticationFailed";
get_error_name(?UNKNOWN_PRODUCER_ID) ->
    "UnknownProducerId";
get_error_name(?REASSIGNMENT_IN_PROGRESS) ->
    "ReassignmentInProgress";
get_error_name(?DELEGATION_TOKEN_AUTH_DISABLED) ->
    "DelegationTokenAuthDisabled";
get_error_name(?DELEGATION_TOKEN_NOT_FOUND) ->
    "DelegationTokenNotFound";
get_error_name(?DELEGATION_TOKEN_OWNER_MISMATCH) ->
    "DelegationTokenOwnerMismatch";
get_error_name(?DELEGATION_TOKEN_REQUEST_NOT_ALLOWED) ->
    "DelegationTokenRequestNotAllowed";
get_error_name(?DELEGATION_TOKEN_AUTHORIZATION_FAILED) ->
    "DelegationTokenAuthorizationFailed";
get_error_name(?DELEGATION_TOKEN_EXPIRED) ->
    "DelegationTokenExpired";
get_error_name(?INVALID_PRINCIPAL_TYPE) ->
    "InvalidPrincipalType";
get_error_name(?NON_EMPTY_GROUP) ->
    "NonEmptyGroup";
get_error_name(?GROUP_ID_NOT_FOUND) ->
    "GroupIdNotFound";
get_error_name(?FETCH_SESSION_ID_NOT_FOUND) ->
    "FetchSessionIdNotFound";
get_error_name(?INVALID_FETCH_SESSION_EPOCH) ->
    "InvalidFetchSessionEpoch";
get_error_name(?LISTENER_NOT_FOUND) ->
    "ListenerNotFound";
get_error_name(?TOPIC_DELETION_DISABLED) ->
    "TopicDeletionDisabled";
get_error_name(?FENCED_LEADER_EPOCH) ->
    "FencedLeaderEpoch";
get_error_name(?UNKNOWN_LEADER_EPOCH) ->
    "UnknownLeaderEpoch";
get_error_name(?UNSUPPORTED_COMPRESSION_TYPE) ->
    "UnsupportedCompressionType";
get_error_name(?STALE_BROKER_EPOCH) ->
    "StaleBrokerEpoch";
get_error_name(?OFFSET_NOT_AVAILABLE) ->
    "OffsetNotAvailable";
get_error_name(?MEMBER_ID_REQUIRED) ->
    "MemberIdRequired";
get_error_name(?PREFERRED_LEADER_NOT_AVAILABLE) ->
    "PreferedLeaderNotAvailable";
get_error_name(?GROUP_MAX_SIZE_REACHED) ->
    "GroupMaxSizeReached";
get_error_name(?FENCED_INSTANCE_ID) ->
    "FencedInstanceId";
get_error_name(?ELIGIBLE_LEADERS_NOT_AVAILABLE) ->
    "EligibleLeadersNotAvailable";
get_error_name(?ELECTION_NOT_NEEDED) ->
    "ElectionNotNeeded";
get_error_name(?NO_REASSIGNMENT_IN_PROGRESS) ->
    "NoReassignmentInProgress";
get_error_name(?GROUP_SUBSCRIBED_TO_TOPIC) ->
    "GroupSubscribedToTopic";
get_error_name(?INVALID_RECORD) ->
    "InvalidRecord";
get_error_name(?UNSTABLE_OFFSET_COMMIT) ->
    "UnstableOffsetCommit";
get_error_name(?THROTTLING_QUOTA_EXCEEDED) ->
    "ThrottlingQuotaExceeded";
get_error_name(?PRODUCER_FENCED) ->
    "ProducerFenced";
get_error_name(?RESOURCE_NOT_FOUND) ->
    "ResourceNotFound";
get_error_name(?DUPLICATE_RESOURCE) ->
    "DuplicateResource";
get_error_name(?UNACCEPTABLE_CREDENTIAL) ->
    "UnacceptableCredential";
get_error_name(?INCONSISTENT_VOTER_SET) ->
    "InconsistentVoterSet";
get_error_name(?INVALID_UPDATE_VERSION) ->
    "InvalidUpdateVersion";
get_error_name(?FEATURE_UPDATE_FAILED) ->
    "FeatureUpdateFailed".

get_error_description(?UNKNOWN_SERVER_ERROR) ->
    "The server experienced an unexpected error when processing the request.";
get_error_description(?NONE) ->
    "None";
get_error_description(?OFFSET_OUT_OF_RANGE) ->
    "The requested offset is not within the range of offsets maintained by the server.";
get_error_description(?CORRUPT_MESSAGE) ->
    "This message has failed its CRC checksum, exceeds the valid size, has a null key " ++
    "for a compacted topic, or is otherwise corrupt.";
get_error_description(?UNKNOWN_TOPIC_OR_PARTITION) ->
    "This server does not host this topic-partition.";
get_error_description(?INVALID_FETCH_SIZE) ->
    "The requested fetch size is invalid.";
get_error_description(?LEADER_NOT_AVAILABLE) ->
    "There is no leader for this topic-partition as we are in the middle of a leadership election.";
get_error_description(?NOT_LEADER_OR_FOLLOWER) ->
    "For requests intended only for the leader, this error indicates that the broker is not " ++
      "the current leader. For requests intended for any replica, this error indicates that the " ++
      "broker is not a replica of the topic partition.";
get_error_description(?REQUEST_TIMED_OUT) ->
    "The request timed out.";
get_error_description(?BROKER_NOT_AVAILABLE) ->
    "The broker is not available.";
get_error_description(?REPLICA_NOT_AVAILABLE) ->
    "The replica is not available for the requested topic-partition. Produce/Fetch requests and " ++
      "other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the " ++
      "broker is not a replica of the topic-partition.";
get_error_description(?MESSAGE_TOO_LARGE) ->
    "The request included a message larger than the max message size the server will accept.";
get_error_description(?STALE_CONTROLLER_EPOCH) ->
    "The controller moved to another broker.";
get_error_description(?OFFSET_METADATA_TOO_LARGE) ->
    "The metadata field of the offset request was too large.";
get_error_description(?NETWORK_EXCEPTION) ->
    "The server disconnected before a response was received.";
get_error_description(?COORDINATOR_LOAD_IN_PROGRESS) ->
    "The coordinator is loading and hence can't process requests.";
get_error_description(?COORDINATOR_NOT_AVAILABLE) ->
    "The coordinator is not available.";
get_error_description(?NOT_COORDINATOR) ->
    "This is not the correct coordinator.";
get_error_description(?INVALID_TOPIC_EXCEPTION) ->
    "The request attempted to perform an operation on an invalid topic.";
get_error_description(?RECORD_LIST_TOO_LARGE) ->
    "The request included message batch larger than the configured segment size on the server.";
get_error_description(?NOT_ENOUGH_REPLICAS) ->
    "Messages are rejected since there are fewer in-sync replicas than required.";
get_error_description(?NOT_ENOUGH_REPLICAS_AFTER_APPEND) ->
    "Messages are written to the log, but to fewer in-sync replicas than required.";
get_error_description(?INVALID_REQUIRED_ACKS) ->
    "Produce request specified an invalid value for required acks.";
get_error_description(?ILLEGAL_GENERATION) ->
    "Specified group generation id is not valid.";
get_error_description(?INCONSISTENT_GROUP_PROTOCOL) ->
    "The group member's supported protocols are incompatible with those of existing members or first " ++
    "group member tried to join with empty protocol type or empty protocol list.";
get_error_description(?INVALID_GROUP_ID) ->
    "The configured groupId is invalid.";
get_error_description(?UNKNOWN_MEMBER_ID) ->
    "The coordinator is not aware of this member.";
get_error_description(?INVALID_SESSION_TIMEOUT) ->
    "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms " ++
    "and group.max.session.timeout.ms).";
get_error_description(?REBALANCE_IN_PROGRESS) ->
    "The group is rebalancing, so a rejoin is needed.";
get_error_description(?INVALID_COMMIT_OFFSET_SIZE) ->
    "The committing offset data size is not valid.";
get_error_description(?TOPIC_AUTHORIZATION_FAILED) ->
    "Topic authorization failed.";
get_error_description(?GROUP_AUTHORIZATION_FAILED) ->
    "Group authorization failed.";
get_error_description(?CLUSTER_AUTHORIZATION_FAILED) ->
    "Cluster authorization failed.";
get_error_description(?INVALID_TIMESTAMP) ->
    "The timestamp of the message is out of acceptable range.";
get_error_description(?UNSUPPORTED_SASL_MECHANISM) ->
    "The broker does not support the requested SASL mechanism.";
get_error_description(?ILLEGAL_SASL_STATE) ->
    "Request is not valid given the current SASL state.";
get_error_description(?UNSUPPORTED_VERSION) ->
    "The version of API is not supported.";
get_error_description(?TOPIC_ALREADY_EXISTS) ->
    "Topic with this name already exists.";
get_error_description(?INVALID_PARTITIONS) ->
    "Number of partitions is below 1.";
get_error_description(?INVALID_REPLICATION_FACTOR) ->
    "Replication factor is below 1 or larger than the number of available brokers.";
get_error_description(?INVALID_REPLICA_ASSIGNMENT) ->
    "Replica assignment is invalid.";
get_error_description(?INVALID_CONFIG) ->
    "Configuration is invalid.";
get_error_description(?NOT_CONTROLLER) ->
    "This is not the correct controller for this cluster.";
get_error_description(?INVALID_REQUEST) ->
    "This most likely occurs because of a request being malformed by the client library or the message was sent " ++
    "to an incompatible broker. See the broker logs for more details.";
get_error_description(?UNSUPPORTED_FOR_MESSAGE_FORMAT) ->
    "The message format version on the broker does not support the request.";
get_error_description(?POLICY_VIOLATION) ->
    "Request parameters do not satisfy the configured policy.";
get_error_description(?OUT_OF_ORDER_SEQUENCE_NUMBER) ->
    "The broker received an out of order sequence number.";
get_error_description(?DUPLICATE_SEQUENCE_NUMBER) ->
    "The broker received a duplicate sequence number.";
get_error_description(?INVALID_PRODUCER_EPOCH) ->
    "Producer attempted to produce with an old epoch.";
get_error_description(?INVALID_TXN_STATE) ->
    "The producer attempted a transactional operation in an invalid state.";
get_error_description(?INVALID_PRODUCER_ID_MAPPING) ->
    "The producer attempted to use a producer id which is not currently assigned to its transactional id.";
get_error_description(?INVALID_TRANSACTION_TIMEOUT) ->
    "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).";
get_error_description(?CONCURRENT_TRANSACTIONS) ->
    "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.";
get_error_description(?TRANSACTION_COORDINATOR_FENCED) ->
    "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.";
get_error_description(?TRANSACTIONAL_ID_AUTHORIZATION_FAILED) ->
    "Transactional Id authorization failed.";
get_error_description(?SECURITY_DISABLED) ->
    "Security features are disabled.";
get_error_description(?OPERATION_NOT_ATTEMPTED) ->
    "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in " ++
    "the batch failed, causing the broker to respond without trying the rest.";
get_error_description(?KAFKA_STORAGE_ERROR) ->
    "Disk error when trying to access log file on the disk.";
get_error_description(?LOG_DIR_NOT_FOUND) ->
    "The user-specified log directory is not found in the broker config.";
get_error_description(?SASL_AUTHENTICATION_FAILED) ->
    "SASL Authentication failed.";
get_error_description(?UNKNOWN_PRODUCER_ID) ->
    "This exception is raised by the broker if it could not locate the producer metadata associated with the " ++
      "producerId in question. This could happen if, for instance, the producer's records were deleted because their " ++
      "retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is " ++
      "removed from the broker, and future appends by the producer will return this exception.";
get_error_description(?REASSIGNMENT_IN_PROGRESS) ->
    "A partition reassignment is in progress.";
get_error_description(?DELEGATION_TOKEN_AUTH_DISABLED) ->
    "Delegation Token feature is not enabled.";
get_error_description(?DELEGATION_TOKEN_NOT_FOUND) ->
    "Delegation Token is not found on server.";
get_error_description(?DELEGATION_TOKEN_OWNER_MISMATCH) ->
    "Specified Principal is not valid Owner/Renewer.";
get_error_description(?DELEGATION_TOKEN_REQUEST_NOT_ALLOWED) ->
    "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.";
get_error_description(?DELEGATION_TOKEN_AUTHORIZATION_FAILED) ->
    "Delegation Token authorization failed.";
get_error_description(?DELEGATION_TOKEN_EXPIRED) ->
    "Delegation Token is expired.";
get_error_description(?INVALID_PRINCIPAL_TYPE) ->
    "Supplied principalType is not supported.";
get_error_description(?NON_EMPTY_GROUP) ->
    "The group is not empty.";
get_error_description(?GROUP_ID_NOT_FOUND) ->
    "The group id does not exist.";
get_error_description(?FETCH_SESSION_ID_NOT_FOUND) ->
    "The fetch session ID was not found.";
get_error_description(?INVALID_FETCH_SESSION_EPOCH) ->
    "The fetch session epoch is invalid.";
get_error_description(?LISTENER_NOT_FOUND) ->
    "There is no listener on the leader broker that matches the listener on which metadata request was processed.";
get_error_description(?TOPIC_DELETION_DISABLED) ->
    "Topic deletion is disabled.";
get_error_description(?FENCED_LEADER_EPOCH) ->
    "The leader epoch in the request is older than the epoch on the broker.";
get_error_description(?UNKNOWN_LEADER_EPOCH) ->
    "The leader epoch in the request is newer than the epoch on the broker.";
get_error_description(?UNSUPPORTED_COMPRESSION_TYPE) ->
    "The requesting client does not support the compression type of given partition.";
get_error_description(?STALE_BROKER_EPOCH) ->
    "Broker epoch has changed.";
get_error_description(?OFFSET_NOT_AVAILABLE) ->
    "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed " ++
    "to be monotonically increasing.";
get_error_description(?MEMBER_ID_REQUIRED) ->
    "The group member needs to have a valid member id before actually entering a consumer group.";
get_error_description(?PREFERRED_LEADER_NOT_AVAILABLE) ->
    "The preferred leader was not available.";
get_error_description(?GROUP_MAX_SIZE_REACHED) ->
    "The consumer group has reached its max size.";
get_error_description(?FENCED_INSTANCE_ID) ->
    "The broker rejected this static consumer since another consumer with the same group.instance.id has registered " ++
    "with a different member.id.";
get_error_description(?ELIGIBLE_LEADERS_NOT_AVAILABLE) ->
    "Eligible topic partition leaders are not available.";
get_error_description(?ELECTION_NOT_NEEDED) ->
    "Leader election not needed for topic partition.";
get_error_description(?NO_REASSIGNMENT_IN_PROGRESS) ->
    "No partition reassignment is in progress.";
get_error_description(?GROUP_SUBSCRIBED_TO_TOPIC) ->
    "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.";
get_error_description(?INVALID_RECORD) ->
    "This record has failed the validation on broker and hence will be rejected.";
get_error_description(?UNSTABLE_OFFSET_COMMIT) ->
    "There are unstable offsets that need to be cleared.";
get_error_description(?THROTTLING_QUOTA_EXCEEDED) ->
    "The throttling quota has been exceeded.";
get_error_description(?PRODUCER_FENCED) ->
    "There is a newer producer with the same transactionalId which fences the current one.";
get_error_description(?RESOURCE_NOT_FOUND) ->
    "A request illegally referred to a resource that does not exist.";
get_error_description(?DUPLICATE_RESOURCE) ->
    "A request illegally referred to the same resource twice.";
get_error_description(?UNACCEPTABLE_CREDENTIAL) ->
    "Requested credential would not meet criteria for acceptability.";
get_error_description(?INCONSISTENT_VOTER_SET) ->
    "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters";
get_error_description(?INVALID_UPDATE_VERSION) ->
    "The given update version was invalid.";
get_error_description(?FEATURE_UPDATE_FAILED) ->
    "Unable to update finalized features due to an unexpected server error.".

get_error_tuple(?UNKNOWN_SERVER_ERROR) ->
    {error, unknown_server_error};
get_error_tuple(?NONE) ->
    {error, none};
get_error_tuple(?OFFSET_OUT_OF_RANGE) ->
    {error, offset_out_of_range};
get_error_tuple(?CORRUPT_MESSAGE) ->
    {error, corrupt_message};
get_error_tuple(?UNKNOWN_TOPIC_OR_PARTITION) ->
    {error, unknown_topic_or_partition};
get_error_tuple(?INVALID_FETCH_SIZE) ->
    {error, invalid_fetch_size};
get_error_tuple(?LEADER_NOT_AVAILABLE) ->
    {error, leader_not_available};
get_error_tuple(?NOT_LEADER_OR_FOLLOWER) ->
    {error, not_leader_or_follower};
get_error_tuple(?REQUEST_TIMED_OUT) ->
    {error, request_timed_out};
get_error_tuple(?BROKER_NOT_AVAILABLE) ->
    {error, broker_not_available};
get_error_tuple(?REPLICA_NOT_AVAILABLE) ->
    {error, replica_not_available};
get_error_tuple(?MESSAGE_TOO_LARGE) ->
    {error, message_too_large};
get_error_tuple(?STALE_CONTROLLER_EPOCH) ->
    {error, stale_controller_epoch};
get_error_tuple(?OFFSET_METADATA_TOO_LARGE) ->
    {error, offset_metadata_too_large};
get_error_tuple(?NETWORK_EXCEPTION) ->
    {error, network_exception};
get_error_tuple(?COORDINATOR_LOAD_IN_PROGRESS) ->
    {error, coordinator_load_in_progress};
get_error_tuple(?COORDINATOR_NOT_AVAILABLE) ->
    {error, coordinator_not_available};
get_error_tuple(?NOT_COORDINATOR) ->
    {error, not_coordinator};
get_error_tuple(?INVALID_TOPIC_EXCEPTION) ->
    {error, invalid_topic_exception};
get_error_tuple(?RECORD_LIST_TOO_LARGE) ->
    {error, record_list_too_large};
get_error_tuple(?NOT_ENOUGH_REPLICAS) ->
    {error, not_enough_replicas};
get_error_tuple(?NOT_ENOUGH_REPLICAS_AFTER_APPEND) ->
    {error, not_enough_replicas_after_append};
get_error_tuple(?INVALID_REQUIRED_ACKS) ->
    {error, invalid_required_acks};
get_error_tuple(?ILLEGAL_GENERATION) ->
    {error, illegal_generation};
get_error_tuple(?INCONSISTENT_GROUP_PROTOCOL) ->
    {error, inconsistent_group_protocol};
get_error_tuple(?INVALID_GROUP_ID) ->
    {error, invalid_group_id};
get_error_tuple(?UNKNOWN_MEMBER_ID) ->
    {error, unknown_member_id};
get_error_tuple(?INVALID_SESSION_TIMEOUT) ->
    {error, invalid_session_timeout};
get_error_tuple(?REBALANCE_IN_PROGRESS) ->
    {error, rebalance_in_progress};
get_error_tuple(?INVALID_COMMIT_OFFSET_SIZE) ->
    {error, invalid_commit_offset_size};
get_error_tuple(?TOPIC_AUTHORIZATION_FAILED) ->
    {error, topic_authorization_failed};
get_error_tuple(?GROUP_AUTHORIZATION_FAILED) ->
    {error, group_authorization_failed};
get_error_tuple(?CLUSTER_AUTHORIZATION_FAILED) ->
    {error, cluster_authorization_failed};
get_error_tuple(?INVALID_TIMESTAMP) ->
    {error, invalid_timestamp};
get_error_tuple(?UNSUPPORTED_SASL_MECHANISM) ->
    {error, unsupported_sasl_mechanism};
get_error_tuple(?ILLEGAL_SASL_STATE) ->
    {error, illegal_sasl_state};
get_error_tuple(?UNSUPPORTED_VERSION) ->
    {error, unsupported_version};
get_error_tuple(?TOPIC_ALREADY_EXISTS) ->
    {error, topic_already_exists};
get_error_tuple(?INVALID_PARTITIONS) ->
    {error, invalid_partitions};
get_error_tuple(?INVALID_REPLICATION_FACTOR) ->
    {error, invalid_replication_factor};
get_error_tuple(?INVALID_REPLICA_ASSIGNMENT) ->
    {error, invalid_replica_assignment};
get_error_tuple(?INVALID_CONFIG) ->
    {error, invalid_config};
get_error_tuple(?NOT_CONTROLLER) ->
    {error, not_controller};
get_error_tuple(?INVALID_REQUEST) ->
    {error, invalid_request};
get_error_tuple(?UNSUPPORTED_FOR_MESSAGE_FORMAT) ->
    {error, unsupported_for_message_format};
get_error_tuple(?POLICY_VIOLATION) ->
    {error, policy_violation};
get_error_tuple(?OUT_OF_ORDER_SEQUENCE_NUMBER) ->
    {error, out_of_order_sequence_number};
get_error_tuple(?DUPLICATE_SEQUENCE_NUMBER) ->
    {error, duplicate_sequence_number};
get_error_tuple(?INVALID_PRODUCER_EPOCH) ->
    {error, invalid_producer_epoch};
get_error_tuple(?INVALID_TXN_STATE) ->
    {error, invalid_txn_state};
get_error_tuple(?INVALID_PRODUCER_ID_MAPPING) ->
    {error, invalid_producer_id_mapping};
get_error_tuple(?INVALID_TRANSACTION_TIMEOUT) ->
    {error, invalid_transaction_timeout};
get_error_tuple(?CONCURRENT_TRANSACTIONS) ->
    {error, concurrent_transactions};
get_error_tuple(?TRANSACTION_COORDINATOR_FENCED) ->
    {error, transaction_coordinator_fenced};
get_error_tuple(?TRANSACTIONAL_ID_AUTHORIZATION_FAILED) ->
    {error, transactional_id_authorization_failed};
get_error_tuple(?SECURITY_DISABLED) ->
    {error, security_disabled};
get_error_tuple(?OPERATION_NOT_ATTEMPTED) ->
    {error, operation_not_attempted};
get_error_tuple(?KAFKA_STORAGE_ERROR) ->
    {error, kafka_storage_error};
get_error_tuple(?LOG_DIR_NOT_FOUND) ->
    {error, log_dir_not_found};
get_error_tuple(?SASL_AUTHENTICATION_FAILED) ->
    {error, sasl_authentication_failed};
get_error_tuple(?UNKNOWN_PRODUCER_ID) ->
    {error, unknown_producer_id};
get_error_tuple(?REASSIGNMENT_IN_PROGRESS) ->
    {error, reassignment_in_progress};
get_error_tuple(?DELEGATION_TOKEN_AUTH_DISABLED) ->
    {error, delegation_token_auth_disabled};
get_error_tuple(?DELEGATION_TOKEN_NOT_FOUND) ->
    {error, delegation_token_not_found};
get_error_tuple(?DELEGATION_TOKEN_OWNER_MISMATCH) ->
    {error, delegation_token_owner_mismatch};
get_error_tuple(?DELEGATION_TOKEN_REQUEST_NOT_ALLOWED) ->
    {error, delegation_token_request_not_allowed};
get_error_tuple(?DELEGATION_TOKEN_AUTHORIZATION_FAILED) ->
    {error, delegation_token_authorization_failed};
get_error_tuple(?DELEGATION_TOKEN_EXPIRED) ->
    {error, delegation_token_expired};
get_error_tuple(?INVALID_PRINCIPAL_TYPE) ->
    {error, invalid_principal_type};
get_error_tuple(?NON_EMPTY_GROUP) ->
    {error, non_empty_group};
get_error_tuple(?GROUP_ID_NOT_FOUND) ->
    {error, group_id_not_found};
get_error_tuple(?FETCH_SESSION_ID_NOT_FOUND) ->
    {error, fetch_session_id_not_found};
get_error_tuple(?INVALID_FETCH_SESSION_EPOCH) ->
    {error, invalid_fetch_session_epoch};
get_error_tuple(?LISTENER_NOT_FOUND) ->
    {error, listener_not_found};
get_error_tuple(?TOPIC_DELETION_DISABLED) ->
    {error, topic_deletion_disabled};
get_error_tuple(?FENCED_LEADER_EPOCH) ->
    {error, fenced_leader_epoch};
get_error_tuple(?UNKNOWN_LEADER_EPOCH) ->
    {error, unknown_leader_epoch};
get_error_tuple(?UNSUPPORTED_COMPRESSION_TYPE) ->
    {error, unsupported_compression_type};
get_error_tuple(?STALE_BROKER_EPOCH) ->
    {error, stale_broker_epoch};
get_error_tuple(?OFFSET_NOT_AVAILABLE) ->
    {error, offset_not_available};
get_error_tuple(?MEMBER_ID_REQUIRED) ->
    {error, member_id_required};
get_error_tuple(?PREFERRED_LEADER_NOT_AVAILABLE) ->
    {error, preferred_leader_not_available};
get_error_tuple(?GROUP_MAX_SIZE_REACHED) ->
    {error, group_max_size_reached};
get_error_tuple(?FENCED_INSTANCE_ID) ->
    {error, fenced_instance_id};
get_error_tuple(?ELIGIBLE_LEADERS_NOT_AVAILABLE) ->
    {error, eligible_leaders_not_available};
get_error_tuple(?ELECTION_NOT_NEEDED) ->
    {error, election_not_needed};
get_error_tuple(?NO_REASSIGNMENT_IN_PROGRESS) ->
    {error, no_reassignment_in_progress};
get_error_tuple(?GROUP_SUBSCRIBED_TO_TOPIC) ->
    {error, group_subscribed_to_topic};
get_error_tuple(?INVALID_RECORD) ->
    {error, invalid_record};
get_error_tuple(?UNSTABLE_OFFSET_COMMIT) ->
    {error, unstable_offset_commit};
get_error_tuple(?THROTTLING_QUOTA_EXCEEDED) ->
    {error, throttling_quota_exceeded};
get_error_tuple(?PRODUCER_FENCED) ->
    {error, producer_fenced};
get_error_tuple(?RESOURCE_NOT_FOUND) ->
    {error, resource_not_found};
get_error_tuple(?DUPLICATE_RESOURCE) ->
    {error, duplicate_resource};
get_error_tuple(?UNACCEPTABLE_CREDENTIAL) ->
    {error, unacceptable_credential};
get_error_tuple(?INCONSISTENT_VOTER_SET) ->
    {error, inconsistent_voter_set};
get_error_tuple(?INVALID_UPDATE_VERSION) ->
    {error, invalid_update_version};
get_error_tuple(?FEATURE_UPDATE_FAILED) ->
    {error, feature_update_failed}.
