-- Optional unoptimized storage comparison; does not connect to Kafka.
-- Same logical columns/types, except String instead of LowCardinality(String),
-- default codecs and no useful sort key. Decimal is retained for fair results.
CREATE DATABASE IF NOT EXISTS homework;

CREATE TABLE IF NOT EXISTS homework.transactions_baseline
(
    transaction_id String,
    source_file String,
    source_row UInt64,
    transaction_time DateTime('UTC'),
    us_state String,
    cat_id String,
    amount Decimal(18, 2)
)
ENGINE = MergeTree
ORDER BY tuple();

-- After ingestion stops, load an EMPTY baseline once:
-- INSERT INTO homework.transactions_baseline SELECT * FROM homework.transactions;
-- Use the 02 query with homework.transactions_baseline as its source.
