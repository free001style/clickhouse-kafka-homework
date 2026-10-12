"""Compare baseline storage, optimized raw storage and per-state aggregates."""
import argparse
import json
from pathlib import Path
import statistics
import time

from app.clickhouse import ClickHouse, read_query
from app.pipeline import normalized_csv

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path, default=Path("/results/benchmark.json"))
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    client = ClickHouse()
    if int(client.query("SELECT count() FROM homework.transactions")[0]) == 0:
        raise RuntimeError("Run the ingestion pipeline before benchmarking")
    client.execute_file(ROOT / "sql/04_baseline.sql")
    client.query("TRUNCATE TABLE homework.transactions_baseline")
    client.query("INSERT INTO homework.transactions_baseline SELECT * FROM homework.transactions")
    # Normalize part counts for this controlled comparison, outside timed runs.
    for table in ("transactions_baseline", "transactions", "largest_transaction_by_state"):
        client.query(f"OPTIMIZE TABLE homework.{table} FINAL")
    raw = read_query(ROOT / "sql/02_largest_transaction_by_state.sql")
    queries = {
        "baseline": raw.replace("homework.transactions", "homework.transactions_baseline"),
        "optimized_raw": raw,
        "aggregate": read_query(ROOT / "sql/03_largest_transaction_by_state_optimized.sql"),
    }
    expected = None
    measurements = {}
    for label, query in queries.items():
        answer, _ = client.query(query, default_format="CSVWithNames", use_query_cache=0)
        actual = normalized_csv(answer)
        if expected is None:
            expected = actual
        if actual != expected:
            raise AssertionError(f"{label} returned different winners")
        samples = []
        for _ in range(args.runs):
            started = time.perf_counter()
            _, summary = client.query(query, default_format="CSVWithNames", use_query_cache=0)
            samples.append({"wall_seconds": time.perf_counter() - started,
                            "read_rows": int(summary.get("read_rows", 0)),
                            "read_bytes": int(summary.get("read_bytes", 0)),
                            "server_elapsed_ns": int(summary.get("elapsed_ns", 0))})
        measurements[label] = {"median_wall_seconds": statistics.median(s["wall_seconds"] for s in samples),
                               "samples": samples}
    storage, _ = client.query("""SELECT table, sum(rows) AS physical_rows,
        sum(data_compressed_bytes) AS compressed_bytes, sum(bytes_on_disk) AS bytes_on_disk
        FROM system.parts WHERE active AND database = 'homework'
        AND table IN ('transactions_baseline', 'transactions', 'largest_transaction_by_state')
        GROUP BY table ORDER BY table FORMAT JSONEachRow""")
    report = {"clickhouse_version": client.query("SELECT version()")[0].strip(),
              "method": "One warm-up, repeated reads, query cache disabled, parts merged before timing; HTTP wall time includes transport. Run with no concurrent ingestion.",
              "results_equal": True, "queries": measurements,
              "storage": [json.loads(line) for line in storage.splitlines()]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
