"""Validate CSV records and publish acknowledged JSONEachRow messages to Kafka."""
import argparse
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import time

REQUIRED = {"transaction_time", "us_state", "cat_id", "amount"}


def normalize(row, source_file, source_row):
    """Keep analytical fields and stable source-row provenance; never round money."""
    missing = REQUIRED - row.keys()
    if missing or None in row or any(row.get(k) is None for k in REQUIRED):
        raise ValueError(f"Malformed CSV record; missing fields: {sorted(missing)}")
    state, category = row["us_state"].strip(), row["cat_id"].strip()
    if not re.fullmatch(r"[A-Z]{2}", state) or not category:
        raise ValueError("State must be two uppercase letters and category must be nonempty")
    try:
        amount = Decimal(row["amount"])
        if not amount.is_finite() or amount < 0 or amount >= Decimal("10000000000000000"):
            raise ValueError("Amount outside nonnegative Decimal(18,2) range")
        if amount != amount.quantize(Decimal("0.01")):
            raise ValueError("Amount has more than two nonzero decimal places")
    except InvalidOperation as exc:
        raise ValueError("Invalid decimal amount") from exc
    stamp = row["transaction_time"].strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?", stamp):
        raise ValueError("Expected YYYY-MM-DD HH:MM[:SS]")
    timestamp = datetime.fromisoformat(stamp)
    if not datetime(1970, 1, 1) <= timestamp <= datetime(2106, 2, 7, 6, 28, 15):
        raise ValueError("Timestamp outside ClickHouse DateTime range")
    return {
        "transaction_id": hashlib.sha256(f"{source_file}:{source_row}".encode()).hexdigest(),
        "source_file": source_file,
        "source_row": source_row,
        "transaction_time": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "us_state": state,
        "cat_id": category,
        "amount": format(amount, ".2f"),
    }


def iter_records(path):
    path = Path(path)
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream, strict=True)
        if not reader.fieldnames or not REQUIRED.issubset(reader.fieldnames):
            raise ValueError(f"{path}: required headers are {sorted(REQUIRED)}")
        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError(f"{path}: duplicate CSV headers")
        for number, row in enumerate(reader, 1):
            try:
                if any(value is None for value in row.values()):
                    raise ValueError("Incorrect number of CSV fields")
                yield normalize(row, path.name, number)
            except (ValueError, TypeError) as exc:
                raise ValueError(f"{path.name}, data row {number}: {exc}") from exc


def preflight(paths):
    """Check every row before publishing anything; only O(1) data memory is needed."""
    paths = [Path(p) for p in paths]
    if len({p.name for p in paths}) != len(paths):
        raise ValueError("Input basenames must be unique (they form transaction identities)")
    sources = []
    for path in paths:
        count = sum(1 for _ in iter_records(path))
        if count == 0:
            raise ValueError(f"{path}: input contains no transactions")
        with path.open("rb") as stream:
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        sources.append({"file": path.name, "rows": count, "sha256": checksum})
    return {"sources": sources, "expected_rows": sum(s["rows"] for s in sources)}


def publish(paths, bootstrap_servers=None, topic="transactions", manifest=None):
    from confluent_kafka import Producer

    manifest = preflight(paths) if manifest is None else manifest
    producer = Producer({
        "bootstrap.servers": bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": "csv-homework-producer",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
        "linger.ms": 20,
        "delivery.timeout.ms": 120000,
        "queue.buffering.max.messages": 100000,
    })
    failures, delivered = [], 0

    def on_delivery(error, message):
        nonlocal delivered
        if error is not None:
            if len(failures) < 10:
                failures.append(str(error))
        else:
            delivered += 1

    sent, started = 0, time.monotonic()
    for path in paths:
        for record in iter_records(path):
            payload = json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode()
            while True:
                if failures:
                    raise RuntimeError(f"Kafka delivery failed: {failures}")
                try:
                    producer.produce(topic, key=record["transaction_id"].encode(), value=payload, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            sent += 1
            producer.poll(0)
            if sent % 100000 == 0:
                print(f"Queued {sent:,} rows", flush=True)
    remaining = producer.flush(130)
    if failures or remaining or delivered != sent or sent != manifest["expected_rows"]:
        raise RuntimeError(f"Incomplete Kafka delivery: sent={sent}, acknowledged={delivered}, pending={remaining}, errors={failures}")
    result = dict(manifest, acknowledged_rows=delivered, topic=topic, elapsed_seconds=round(time.monotonic() - started, 3))
    print(f"Kafka acknowledged all {delivered:,} rows", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", nargs="+", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--manifest", type=Path, default=Path("results/producer_manifest.json"))
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    manifest = preflight(args.csv)
    if not args.validate_only:
        manifest = publish(args.csv, args.bootstrap_servers, args.topic, manifest)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
