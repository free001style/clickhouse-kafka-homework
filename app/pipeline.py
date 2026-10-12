"""Run the complete ingestion, wait for ClickHouse, export and cross-check results."""
import argparse
import csv
from decimal import Decimal
import io
import json
from pathlib import Path
import time

from app.clickhouse import ClickHouse, read_query
from app.producer import preflight, publish

ROOT = Path(__file__).resolve().parent.parent


def normalized_csv(text):
    rows = list(csv.DictReader(io.StringIO(text)))
    return sorted((r["us_state"], r["cat_id"], Decimal(r["max_amount"]), r["transaction_id"]) for r in rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", nargs="+", type=Path, default=[Path("/data/train.csv")])
    parser.add_argument("--output-dir", type=Path, default=Path("/results"))
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()
    manifest = preflight(args.csv)
    print(f"Validated {manifest['expected_rows']:,} source rows", flush=True)
    client = ClickHouse()
    client.execute_file(ROOT / "sql/01_schema.sql")
    count = int(client.query("SELECT count() FROM homework.transactions")[0])
    if count:
        raise RuntimeError("The transaction table is not empty. To run a clean import, use 'docker compose down -v' first. This deletes this project's Kafka and ClickHouse data.")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = publish(args.csv, manifest=manifest)
    (args.output_dir / "producer_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    deadline = time.monotonic() + args.timeout
    expected = manifest["expected_rows"]
    while True:
        data, _ = client.query("SELECT count(), uniqExact(transaction_id) FROM homework.transactions FORMAT JSONEachRow")
        counts = json.loads(data)
        physical, unique = int(counts["count()"]), int(counts["uniqExact(transaction_id)"])
        print(f"ClickHouse contains {unique:,}/{expected:,} unique rows ({physical:,} physical)", flush=True)
        if unique == expected:
            break
        if unique > expected:
            raise RuntimeError("Unexpected extra rows: reset the project volumes and retry")
        if time.monotonic() >= deadline:
            raise TimeoutError("ClickHouse ingestion timed out; inspect 'docker compose logs clickhouse kafka' and sql/05_diagnostics.sql")
        time.sleep(2)
    base, _ = client.query(read_query(ROOT / "sql/02_largest_transaction_by_state.sql"), default_format="CSVWithNames")
    # Aggregate MV is fed synchronously, but a short retry also handles an in-flight block.
    while True:
        optimized, _ = client.query(read_query(ROOT / "sql/03_largest_transaction_by_state_optimized.sql"), default_format="CSVWithNames")
        if normalized_csv(base) == normalized_csv(optimized):
            break
        if time.monotonic() >= deadline:
            raise AssertionError("Raw-table and aggregate-table queries disagree")
        time.sleep(1)
    from app.reference import write_reference
    reference_path = args.output_dir / "reference.csv"
    write_reference(args.csv, reference_path)
    if normalized_csv(base) != normalized_csv(reference_path.read_text()):
        raise AssertionError("ClickHouse result differs from the independent CSV reference")
    (args.output_dir / "largest_transaction_by_state.csv").write_text(optimized)
    manifest.update({"clickhouse_physical_rows": physical, "clickhouse_unique_rows": unique,
                     "state_count": len(normalized_csv(base)), "raw_optimized_reference_match": True,
                     "result_origin": "ClickHouse SQL after Kafka ingestion"})
    (args.output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Verified and exported {manifest['state_count']} state winners to {args.output_dir}", flush=True)


if __name__ == "__main__":
    main()
