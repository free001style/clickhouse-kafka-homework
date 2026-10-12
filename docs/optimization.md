# Storage and query optimization

## Meaning of the result

The answer is the category of the **largest individual transaction** in each
`us_state`. It is not the category with the largest sum of transactions.
`sql/02_largest_transaction_by_state.sql` returns one row per observed state:
`us_state`, `cat_id`, `max_amount`, and `transaction_id`.

When amounts tie, the lexicographically greatest `transaction_id` wins. The
producer derives that ID from SHA-256 of `source_file + ':' + source_row`, where
the first CSV data record is row 1. Replaying an unchanged source file therefore
preserves the result. File names and record order must remain stable; this is a
source-record identity, not a business transaction key.

The query uses `argMax((cat_id, amount, transaction_id),
(amount, transaction_id))`. Keeping all returned fields in the same tuple ensures
they come from the same winning record. A second ordering field makes ties
deterministic for distinct source IDs. ClickHouse documents tuple comparison for
[`argMax`](https://clickhouse.com/docs/reference/functions/aggregate-functions/argMax).

## Data flow and storage decisions

Each validated CSV row produces one JSON Kafka message. The Kafka table feeds
`homework.transactions` through `homework.kafka_to_transactions_mv`. A second
materialized view maintains `homework.largest_transaction_by_state`. The Kafka
consumer view is created last so that every newly consumed row can reach the
aggregate table.

Only the three analytical fields, transaction time, and source provenance are
retained. Names, addresses, coordinates, and other columns are unnecessary for
this question. Both train and test CSV files use the same retained schema;
the training-only `target` column is not required.

| Decision | Purpose and limitation |
| --- | --- |
| `MergeTree` durable row table | Keeps the necessary source records for validation and future reaggregation. Kafka's table engine itself is a stream. |
| `Decimal(18, 2)` amount | Stores exact cents. The producer validates precision and sends a decimal string, avoiding float conversion. |
| `LowCardinality(String)` state and category | Dictionary encodes repeated labels. Appropriate for the small state/category domains; the actual benefit is measured below. |
| `DateTime('UTC')` | Parses the source's timezone-free timestamp consistently. UTC is a storage convention; the dataset does not establish each transaction's real timezone. |
| `ORDER BY (us_state, transaction_time, transaction_id)` | Groups similar states and times for compression, and supports state/time filtering. It does not let an all-state maximum query skip the complete dataset. |
| `ZSTD(1)` plus `Delta` for time | A conservative compression configuration. Delta acts on sorted timestamps; compression ratios depend on the data. |
| No partition key | Avoids many small partitions for this bounded homework dataset. Monthly partitions would be useful for retention or partition removal at a different scale. |
| `AggregatingMergeTree ORDER BY us_state` | Stores partial winning-transaction states per state and insert block, then combines them during background merges. |

See the official
[`LowCardinality`](https://clickhouse.com/docs/reference/data-types/lowcardinality)
and [`Decimal`](https://clickhouse.com/docs/reference/data-types/decimal)
documentation for their representation and precision rules.

## Optimized query

`sql/03_largest_transaction_by_state_optimized.sql` uses
`argMaxMerge(winner_state)` and `GROUP BY us_state`. It combines partial states at
read time, including those that have not yet been merged into one disk part.
Neither `FINAL` nor `OPTIMIZE TABLE ... FINAL` is needed for correctness.

The aggregate materialized view performs `argMaxState` on every inserted block.
The expected saving is fewer rows and bytes read for this repeated, fixed query.
The cost is extra work during insertion and additional aggregate storage. This
is a query-specific optimization: new groupings or arbitrary filters still need
the durable row table or another aggregate design. See
[`AggregatingMergeTree`](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/aggregatingmergetree)
and [incremental materialized views](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view).

## Delivery, recovery, and correctness boundaries

The pipeline uses Kafka's ordinary consumer offsets and must be treated as
**at least once**. A failure between an insert and offset commit can replay rows.
The durable `MergeTree` table does not enforce uniqueness. The same immutable
record repeated any number of times does not change `argMax` or `argMaxMerge`,
but it does affect raw `count()` and `sum()` queries. The producer's stable IDs
let `uniqExact(transaction_id)` distinguish source records from delivery copies.

The group name is fixed at `homework-clickhouse-v1`. Do not change it or reset
offsets casually: either action can reconsume retained messages. Do not query
the Kafka table as if it were a historical table. Check consumer errors and
offsets using `system.kafka_consumers`; see the
[Kafka engine documentation](https://clickhouse.com/docs/reference/engines/table-engines/integrations/kafka).

Malformed producer input must stop with an error before that row is sent.
ClickHouse uses `kafka_skip_broken_messages = 0`, so it does not silently discard
malformed Kafka messages. A poison message can stall ingestion and must be
investigated. The schema assumes messages originate from the validated producer;
it is not a validation boundary for arbitrary external writers.

Incremental views process **new inserts**, not past rows or subsequent updates
and deletes. `IF NOT EXISTS` makes initialization repeatable but does not migrate
an existing schema. If adding the aggregate to already populated raw storage,
stop ingestion, create the target and view, and backfill once:

```sql
INSERT INTO homework.largest_transaction_by_state
SELECT
    us_state,
    argMaxState(
        tuple(toString(cat_id), amount, transaction_id),
        tuple(amount, transaction_id)
    ) AS winner_state
FROM homework.transactions
GROUP BY us_state;
```

Then resume ingestion. Repeating this exact max-state backfill is idempotent for
the result, although it adds work and physical states. If a previously winning
transaction is corrected or deleted, recreate/rebuild the aggregate from the
current durable rows while ingestion is paused: an old maximum cannot be
retracted by appending a lower amount. Immutable input is therefore required.

## Reproducible performance comparison

Three variants separate ordinary scanning, storage choices, and preaggregation:

| Variant | Data and query |
| --- | --- |
| Baseline storage | Optional `sql/04_baseline.sql`: plain strings, default codecs, `ORDER BY tuple()`; run query 02 against `homework.transactions_baseline`. |
| Optimized row storage | `homework.transactions`; run query 02 unchanged. |
| Preaggregated result | `homework.largest_transaction_by_state`; run query 03. |

Use the same source rows in all variants. After ingestion has finished and the
distinct row count matches the producer's source count, create the optional
baseline and fill the **empty** table once:

```sql
INSERT INTO homework.transactions_baseline
SELECT * FROM homework.transactions;
```

For an embedded ClickHouse or standalone server without Kafka, initialize
`sql/00_storage.sql` and insert the producer's JSON records directly into
`homework.transactions` with `FORMAT JSONEachRow`; the downstream materialized
view still runs. This validates storage/query behavior, not Kafka delivery.

Before timing, compare all three ordered result sets exactly, including the
winning IDs and decimal amounts. Warm up each query once, then run each at least
five times in alternating order. Disable the query result cache, use the same
`max_threads` setting, stop further ingestion, and retain per-run measurements.
Use distinct query IDs such as `benchmark-baseline-1`, `benchmark-base-1`, and
`benchmark-aggregate-1`; `clickhouse-client --query_id` sets them. Report the
median elapsed time together with `read_rows`, `read_bytes`, and peak
`memory_usage` from `system.query_log`. Keep cold and warm results separate if
both are measured.

`sql/05_diagnostics.sql` includes the log query and `system.parts` storage totals.
Compare total bytes for the row table **plus its aggregate** against the
baseline; reporting only the tiny aggregate hides the cost of retained raw
storage. Record server version, hardware, row counts, active parts, and query
settings. Background merges and filesystem caching can change timings and
compression, so do not attribute every difference to the schema alone.

No speedup or compression ratio is asserted without an actual ClickHouse run.
The Python reference result and producer tests verify data logic separately;
they cannot establish ClickHouse performance or end-to-end Kafka delivery.
