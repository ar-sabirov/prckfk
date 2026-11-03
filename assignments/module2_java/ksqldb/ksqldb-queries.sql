-- Executed automatically by the ksqldb-init Compose service.
-- For manual execution, run this file in the ksqlDB CLI. In Kafka UI, run the
-- CREATE statements in order with stream property auto.offset.reset=earliest.
-- IF NOT EXISTS permits adding missing queries to an existing installation;
-- it does not replace existing definitions or reset their history.
SET 'auto.offset.reset' = 'earliest';

-- Analytics consume censored messages with final recipient lists after blocking.
CREATE STREAM IF NOT EXISTS FILTERED_MESSAGES_STREAM (
    SENDER_ID STRING KEY,
    MESSAGE STRUCT<MESSAGE_ID STRING, SENDER_ID STRING, TEXT STRING>,
    RECIPIENT_IDS ARRAY<STRING>
) WITH (
    KAFKA_TOPIC='filtered_messages',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='PROTOBUF_NOSR',
    PARTITIONS=3
);

-- Count the complete message, including messages with no eligible recipients.
CREATE TABLE IF NOT EXISTS USER_MESSAGE_COUNTS WITH (
    KAFKA_TOPIC='user_message_counts',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, COUNT(*) AS MESSAGE_COUNT
FROM FILTERED_MESSAGES_STREAM
GROUP BY SENDER_ID
EMIT CHANGES;

-- Only the recipient branch expands the array into individual relationships.
CREATE STREAM IF NOT EXISTS RECIPIENT_EVENTS WITH (
    KAFKA_TOPIC='recipient_events',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, EXPLODE(RECIPIENT_IDS) AS RECIPIENT_ID
FROM FILTERED_MESSAGES_STREAM
EMIT CHANGES;

-- A composite key gives each historical sender-recipient pair exactly one row.
CREATE TABLE IF NOT EXISTS SENDER_RECIPIENTS WITH (
    KAFKA_TOPIC='sender_recipients',
    KEY_FORMAT='JSON', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, RECIPIENT_ID, COUNT(*) AS SELECTION_COUNT
FROM RECIPIENT_EVENTS
GROUP BY SENDER_ID, RECIPIENT_ID
EMIT CHANGES;

-- Count TABLE rows, not the stream of updates to those rows. Repeated delivery
-- to an existing recipient updates a pair but does not add a new pair.
CREATE TABLE IF NOT EXISTS USER_RECIPIENT_COUNTS WITH (
    KAFKA_TOPIC='user_recipient_counts',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, COUNT(*) AS UNIQUE_RECIPIENT_COUNT
FROM SENDER_RECIPIENTS
GROUP BY SENDER_ID
EMIT CHANGES;

-- The left join preserves senders who have sent only to empty recipient lists.
CREATE TABLE IF NOT EXISTS USER_STATISTICS WITH (
    KAFKA_TOPIC='user_statistics',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT M.SENDER_ID AS SENDER_ID,
       M.MESSAGE_COUNT AS MESSAGE_COUNT,
       COALESCE(R.UNIQUE_RECIPIENT_COUNT, CAST(0 AS BIGINT)) AS UNIQUE_RECIPIENT_COUNT
FROM USER_MESSAGE_COUNTS M
LEFT JOIN USER_RECIPIENT_COUNTS R ON M.SENDER_ID = R.SENDER_ID
EMIT CHANGES;

-- Global statistics use existing TABLE snapshots, preserving their history even
-- when older delivery events have expired. SUM replaces a sender's previous
-- contribution when their count changes; it does not sum the stream of updates.
CREATE TABLE IF NOT EXISTS GLOBAL_MESSAGE_COUNTS WITH (
    KAFKA_TOPIC='global_message_counts',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON', PARTITIONS=1
) AS
SELECT 'all' AS STATISTICS_KEY, SUM(MESSAGE_COUNT) AS MESSAGE_COUNT
FROM USER_MESSAGE_COUNTS
GROUP BY 'all'
EMIT CHANGES;

-- One row per recipient, regardless of how many senders have reached them.
-- Counting pair TABLE rows also keeps repeated selections from the same sender
-- from inflating SENDER_COUNT. No additional EXPLODE branch is required.
CREATE TABLE IF NOT EXISTS GLOBAL_RECIPIENTS WITH (
    KAFKA_TOPIC='global_recipients',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON', PARTITIONS=3
) AS
SELECT RECIPIENT_ID, COUNT(*) AS SENDER_COUNT
FROM SENDER_RECIPIENTS
GROUP BY RECIPIENT_ID
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS GLOBAL_RECIPIENT_COUNTS WITH (
    KAFKA_TOPIC='global_recipient_counts',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON', PARTITIONS=1
) AS
SELECT 'all' AS STATISTICS_KEY, COUNT(*) AS UNIQUE_RECIPIENT_COUNT
FROM GLOBAL_RECIPIENTS
GROUP BY 'all'
EMIT CHANGES;

-- Preserve the total when every processed message has an empty recipient list.
CREATE TABLE IF NOT EXISTS GLOBAL_STATISTICS WITH (
    KAFKA_TOPIC='global_statistics',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON', PARTITIONS=1
) AS
SELECT M.STATISTICS_KEY AS STATISTICS_KEY,
       M.MESSAGE_COUNT AS MESSAGE_COUNT,
       COALESCE(R.UNIQUE_RECIPIENT_COUNT, CAST(0 AS BIGINT)) AS UNIQUE_RECIPIENT_COUNT
FROM GLOBAL_MESSAGE_COUNTS M
LEFT JOIN GLOBAL_RECIPIENT_COUNTS R ON M.STATISTICS_KEY = R.STATISTICS_KEY
EMIT CHANGES;
