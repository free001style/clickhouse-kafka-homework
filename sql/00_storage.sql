-- Durable tables and aggregate view only: safe to use without Kafka.
-- This block is repeated in 01_schema.sql so that deployment needs one init file.
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

-- Install this BEFORE enabling the Kafka consumer.
-- The category, amount and id are kept in a single tuple from the same row.
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
