# homework2

CSV records pass through a Python producer, Kafka, and a ClickHouse materialized view.

## Run

Requirements: Docker with Compose v2.20+ and about 4 GB of available memory.

```bash
git clone https://github.com/free001style/clickhouse-kafka-homework.git
cd clickhouse-kafka-homework
```

Extract `train.csv` from the supplied archive into `data/`. Raw datasets are excluded from Git.

```bash
docker compose up -d --wait kafka clickhouse
docker compose run --build --rm pipeline --csv /data/train.csv --output-dir /results/train_run
```

The pipeline imports the data, compares both SQL queries with a Python reference,
and writes `results/train_run/largest_transaction_by_state.csv`.
Output columns: `us_state,cat_id,max_amount,transaction_id`.
Ties select the lexicographically greatest transaction ID.

Before another import, reset the stack. **This deletes previously imported data.**

```bash
docker compose down -v
docker compose up -d --wait kafka clickhouse
```

For a quick demo on a fresh stack, use the included 20-row dataset:

```bash
docker compose run --build --rm pipeline --csv /data/demo.csv --output-dir /results/demo
```

## Files

| Purpose | File |
| --- | --- |
| CSV → Kafka producer | [app/producer.py](app/producer.py) |
| ClickHouse tables and ingestion | [sql/01_schema.sql](sql/01_schema.sql) |
| Base query | [sql/02_largest_transaction_by_state.sql](sql/02_largest_transaction_by_state.sql) |
| Optimized query | [sql/03_largest_transaction_by_state_optimized.sql](sql/03_largest_transaction_by_state_optimized.sql) |
| Full training-data result | [results/largest_transaction_by_state.csv](results/largest_transaction_by_state.csv) |
| Combined train/test result | [results/largest_transaction_by_state_all.csv](results/largest_transaction_by_state_all.csv) |

Storage uses `LowCardinality`, ZSTD compression, and Delta encoding for timestamps.
An `AggregatingMergeTree` materialized view maintains per-state `argMaxState`
values, allowing the optimized query to read compact aggregates instead of all
transactions. Details: [docs/optimization.md](docs/optimization.md).

## Verification

```bash
python -m unittest discover -s tests -v
# Run after importing data:
docker compose run --rm --entrypoint python pipeline -m app.benchmark --runs 5 --output /results/benchmark.json
```

The committed full-data CSVs were computed and checked independently;
container integration was verified on the demo. [Validation details](docs/validation.md).
