-- Ingested records vs stable source identities; retries may duplicate raw rows.
SELECT
    count() AS stored_rows,
    uniqExact(transaction_id) AS distinct_transactions,
    count() - uniqExact(transaction_id) AS duplicate_rows,
    uniqExact(us_state) AS states,
    uniqExact(cat_id) AS categories,
    min(transaction_time) AS first_transaction,
    max(transaction_time) AS last_transaction
FROM homework.transactions;

-- Check active storage, including the extra aggregate table; raw row counts in
-- AggregatingMergeTree are partial states, not the number of logical states.
SELECT
    table,
    sum(rows) AS physical_rows,
    sum(data_compressed_bytes) AS compressed_bytes,
    sum(data_uncompressed_bytes) AS uncompressed_bytes,
    sum(bytes_on_disk) AS total_bytes_on_disk,
    count() AS active_parts
FROM system.parts
WHERE active AND database = 'homework'
GROUP BY table
ORDER BY table;

-- Consumer assignments, offsets and errors; inspect if ingestion stalls.
SELECT *
FROM system.kafka_consumers
WHERE database = 'homework' AND table = 'transactions_kafka'
FORMAT Vertical;

-- After running benchmark queries with query_id beginning "benchmark-".
SYSTEM FLUSH LOGS;
SELECT
    query_id,
    query_duration_ms,
    read_rows,
    read_bytes,
    memory_usage,
    result_rows
FROM system.query_log
WHERE type = 'QueryFinish'
    AND is_initial_query
    AND startsWith(query_id, 'benchmark-')
ORDER BY event_time_microseconds DESC
LIMIT 100;
