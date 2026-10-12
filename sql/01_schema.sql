-- Complete fresh-install schema. Run using clickhouse-client --multiquery.
-- The storage block also exists separately in 00_storage.sql for offline tests.
CREATE DATABASE IF NOT EXISTS homework;

CREATE TABLE IF NOT EXISTS homework.transactions
(
    transaction_id String CODEC(ZSTD(1)),
    source_file String CODEC(ZSTD(1)),
    source_row UInt64 CODEC(ZSTD(1)),
    transaction_time DateTime('UTC') CODEC(Delta, ZSTD(1)),
    us_state LowCardinality(String) CODEC(ZSTD(1)),
    cat_id LowCardinality(String) CODEC(ZSTD(1)),
    amount Decimal(18, 2) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (us_state, transaction_time, transaction_id);

CREATE TABLE IF NOT EXISTS homework.largest_transaction_by_state
(
    us_state LowCardinality(String),
    winner_state AggregateFunction(
        argMax,
        Tuple(String, Decimal(18, 2), String),
        Tuple(Decimal(18, 2), String)
    )
)
ENGINE = AggregatingMergeTree
ORDER BY us_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.transactions_to_largest_mv
TO homework.largest_transaction_by_state
AS
SELECT
    us_state,
    argMaxState(
        tuple(toString(cat_id), amount, transaction_id),
        tuple(amount, transaction_id)
    ) AS winner_state
FROM homework.transactions
GROUP BY us_state;

-- Kafka is a stream, not durable analytical storage. Never SELECT it to
-- produce the homework answer: query transactions or its aggregate instead.
CREATE TABLE IF NOT EXISTS homework.transactions_kafka
(
    transaction_id String,
    source_file String,
    source_row UInt64,
    transaction_time String,
    us_state String,
    cat_id String,
    amount String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'homework-clickhouse-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 0;

-- Attaching this last starts background ingestion after all destinations exist.
-- Amount arrives as a decimal string, never a binary floating-point number.
CREATE MATERIALIZED VIEW IF NOT EXISTS homework.kafka_to_transactions_mv
TO homework.transactions
AS
SELECT
    transaction_id,
    source_file,
    source_row,
    toDateTime(transaction_time, 'UTC') AS transaction_time,
    us_state,
    cat_id,
    toDecimal64(amount, 2) AS amount
FROM homework.transactions_kafka;
