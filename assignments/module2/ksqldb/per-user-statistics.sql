-- Run this file in the ksqlDB CLI. In Kafka UI, run the CREATE statements in
-- order and set the stream property auto.offset.reset=earliest for each request.
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS CHAT_DELIVERIES (
    SENDER_ID STRING KEY,
    MESSAGE STRUCT<MESSAGE_ID STRING, SENDER_ID STRING, TEXT STRING>,
    RECIPIENT_IDS ARRAY<STRING>
) WITH (
    KAFKA_TOPIC='module2.chat.deliveries',
    KEY_FORMAT='KAFKA',
    VALUE_FORMAT='PROTOBUF_NOSR',
    PARTITIONS=3
);

-- Count the complete message, including messages with no eligible recipients.
CREATE TABLE CHAT_MESSAGE_COUNTS WITH (
    KAFKA_TOPIC='module2.chat.stats.message-counts',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, COUNT(*) AS MESSAGE_COUNT
FROM CHAT_DELIVERIES
GROUP BY SENDER_ID
EMIT CHANGES;

-- Only the recipient branch expands the array into individual relationships.
CREATE STREAM CHAT_RECIPIENT_EVENTS WITH (
    KAFKA_TOPIC='module2.chat.stats.recipient-events',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, EXPLODE(RECIPIENT_IDS) AS RECIPIENT_ID
FROM CHAT_DELIVERIES
EMIT CHANGES;

-- A composite key gives each historical sender-recipient pair exactly one row.
CREATE TABLE CHAT_SENDER_RECIPIENTS WITH (
    KAFKA_TOPIC='module2.chat.stats.sender-recipients',
    KEY_FORMAT='JSON', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, RECIPIENT_ID, COUNT(*) AS SELECTION_COUNT
FROM CHAT_RECIPIENT_EVENTS
GROUP BY SENDER_ID, RECIPIENT_ID
EMIT CHANGES;

-- Count TABLE rows, not the stream of updates to those rows. Repeated delivery
-- to an existing recipient updates a pair but does not add a new pair.
CREATE TABLE CHAT_RECIPIENT_COUNTS WITH (
    KAFKA_TOPIC='module2.chat.stats.recipient-counts',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT SENDER_ID, COUNT(*) AS UNIQUE_RECIPIENT_COUNT
FROM CHAT_SENDER_RECIPIENTS
GROUP BY SENDER_ID
EMIT CHANGES;

-- The left join preserves senders who have sent only to empty recipient lists.
CREATE TABLE CHAT_USER_STATISTICS WITH (
    KAFKA_TOPIC='module2.chat.user-statistics',
    KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON'
) AS
SELECT M.SENDER_ID AS SENDER_ID,
       M.MESSAGE_COUNT AS MESSAGE_COUNT,
       COALESCE(R.UNIQUE_RECIPIENT_COUNT, CAST(0 AS BIGINT)) AS UNIQUE_RECIPIENT_COUNT
FROM CHAT_MESSAGE_COUNTS M
LEFT JOIN CHAT_RECIPIENT_COUNTS R ON M.SENDER_ID = R.SENDER_ID
EMIT CHANGES;
