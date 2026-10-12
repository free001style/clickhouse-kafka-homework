# Validation status

Updated on 2026-10-12. This file distinguishes completed local and GitHub Actions
checks from the remaining full-dataset runtime checks.

## Completed

- **46 standard-library unit tests passed** with
  `python -m unittest discover -s tests -v` on Python 3.12.
- Producer preflight validated **all 1,048,575 rows** in train.csv and test.csv.
  Exact input checksums and counts are in `results/input_validation.json`.
- Full source audit found no malformed CSV widths, missing fields, invalid
  amounts/timestamps, negative amounts or fractional cents in the supplied data.
  Details are in `results/dataset_summary.json`.
- Both 51-row result files exactly matched an independent pandas calculation
  using integer cents. The committed outputs are CSV reference results.
- All Python source, Docker Compose/CI YAML and ClickHouse XML parsed
  successfully. Container dependency wiring was inspected.
- Durable storage DDL is identical in `00_storage.sql` and `01_schema.sql`.
  SQL was reviewed against official ClickHouse documentation, including aggregate
  state types, deterministic ties, view creation order and at-least-once behavior.
- **Real Docker/Kafka/ClickHouse integration passed** in
  [GitHub Actions run 38281744250](https://github.com/free001style/clickhouse-kafka-homework/actions/runs/38281744250)
  (job `114920399783`). The job reran all 46 unit tests, started the real Kafka
  and ClickHouse containers, published all 20 synthetic demo records, and verified
  all expected unique transaction IDs in ClickHouse.
- **Native ClickHouse SQL execution passed** in that Docker run: both the raw
  query and the optimized aggregate query exactly matched the independent Python
  reference on the demo dataset.
- **The benchmark command passed** with `--runs 2` on the demo. This exercises
  the baseline, optimized row-storage and aggregate-query measurement path; the
  tiny demo is not evidence of a representative full-dataset speedup.
- The same job successfully uploaded the `kafka-clickhouse-demo-results`
  artifact containing result CSVs, run manifests and the demo benchmark, then
  completed container cleanup. Download the artifact from the linked run for
  the actual integration evidence.

Tests cover malformed CSV, optional target, quoted commas, exact money bounds,
nonfinite values, timestamp bounds, duplicate input names, stable IDs, validation
before side effects, mocked Kafka backpressure and failed acknowledgements,
ClickHouse HTTP errors, quoted UInt64 counts, nonempty-table rejection,
ingestion timeout and result disagreement. The mocked unit tests are complemented
by the successful real broker/database integration described above.

## Remaining full-dataset checks

- Import **786,431 training rows**, or **1,048,575 combined train/test rows**,
  through Kafka into ClickHouse and retain the exported CSV and run manifest.
  The source validation and reference calculations already cover all supplied
  rows; the completed container integration used the 20-row synthetic demo.
- Measure runtime and storage on that full-size import using `app.benchmark`.
  No full-size timings, compression ratios or speedup claims are asserted. The
  completed demo benchmark establishes that the command runs successfully.

The original development workspace still lacks Docker, Podman and standalone
ClickHouse binaries. Its attempted embedded chDB check crashed during engine
startup before any SQL ran and is not counted as a passing check. The successful
Docker and native SQL checks instead ran on the GitHub Actions host.

To complete the full-dataset runtime checks, follow the README train.csv or
combined-input commands on a Docker host and retain `run_manifest.json`, the
exported CSV and `benchmark.json`.
