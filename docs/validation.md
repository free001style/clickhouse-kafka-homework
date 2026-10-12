# Validation status

Prepared on 2026-10-12. This file distinguishes checks actually run from checks
that require a Docker host.

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

Tests cover malformed CSV, optional target, quoted commas, exact money bounds,
nonfinite values, timestamp bounds, duplicate input names, stable IDs, validation
before side effects, mocked Kafka backpressure and failed acknowledgements,
ClickHouse HTTP errors, quoted UInt64 counts, nonempty-table rejection,
ingestion timeout and result disagreement. Mocked tests do not establish broker
or database compatibility.

## Not yet completed

- **Actual Docker/Kafka/ClickHouse integration run.** Docker, Podman and the
  ClickHouse server/client are not installed in the development workspace.
- **Native ClickHouse SQL execution.** An attempted embedded chDB check crashed
  during engine startup before any SQL ran. It is not counted as a passing check.
- **Actual performance/storage measurements.** No timings or speedup claims
  were invented. `app.benchmark` generates measurements on a running stack.

The GitHub Actions workflow performs the real Kafka → ClickHouse demo and
checks its result. Run it on GitHub, or use the README quick start on a Docker
host. For the full assignment, run the train.csv command and retain its
`run_manifest.json`, exported CSV and optional `benchmark.json`.
