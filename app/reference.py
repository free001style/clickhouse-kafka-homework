#!/usr/bin/env python3
"""CSV-only reference oracle for largest transaction per US state.

This module deliberately has no Kafka/ClickHouse imports. It computes expected
results directly from source CSV records with exact Decimal arithmetic. Record
identity is SHA-256("<CSV basename>:<one-based data-row number>"); the header is
not a data row. Equal amounts choose the lexicographically greatest identity,
matching ClickHouse argMax(..., tuple(amount, transaction_id)).
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path
from typing import Iterable

REQUIRED_COLUMNS = frozenset({"us_state", "cat_id", "amount"})
OUTPUT_COLUMNS = ("us_state", "cat_id", "max_amount", "transaction_id")


@dataclass(frozen=True)
class Winner:
    us_state: str
    cat_id: str
    amount: Decimal
    transaction_id: str

    def as_row(self) -> dict[str, str]:
        return {"us_state": self.us_state, "cat_id": self.cat_id,
                "max_amount": format(self.amount, ".2f"),
                "transaction_id": self.transaction_id}


def transaction_id(filename: str, row_number: int) -> str:
    """Return deterministic identity; numbering begins at the first data row."""
    return hashlib.sha256(f"{filename}:{row_number}".encode("utf-8")).hexdigest()


def compute_winners(csv_paths: Iterable[Path | str]) -> dict[str, Winner]:
    """Read all source records and fail on invalid rows instead of skipping them."""
    paths = [Path(path) for path in csv_paths]
    names = [path.name for path in paths]
    if len(names) != len(set(names)):
        raise ValueError("CSV basenames must be unique: they are part of record identity")
    winners: dict[str, Winner] = {}
    for path in paths:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream, strict=True)
            fields = reader.fieldnames or []
            if len(fields) != len(set(fields)):
                raise ValueError(f"{path}: duplicate CSV column names")
            if not REQUIRED_COLUMNS.issubset(fields):
                missing = ", ".join(sorted(REQUIRED_COLUMNS.difference(fields)))
                raise ValueError(f"{path}: missing required columns: {missing}")
            for row_number, row in enumerate(reader, start=1):
                context = f"{path.name}: data row {row_number}"
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(f"{context}: incorrect number of CSV fields")
                state, category = row["us_state"], row["cat_id"]
                if not state or not category:
                    raise ValueError(f"{context}: empty state or category")
                try:
                    amount = Decimal(row["amount"])
                except InvalidOperation as error:
                    raise ValueError(f"{context}: invalid amount") from error
                if not amount.is_finite() or amount < 0:
                    raise ValueError(f"{context}: amount must be finite and non-negative")
                if amount != amount.quantize(Decimal("0.01")):
                    raise ValueError(f"{context}: amount has fractional cents")
                candidate = Winner(state, category, amount,
                                   transaction_id(path.name, row_number))
                previous = winners.get(state)
                if previous is None or (candidate.amount, candidate.transaction_id) > (
                        previous.amount, previous.transaction_id):
                    winners[state] = candidate
    return winners


def write_results(winners: dict[str, Winner], output: Path | str) -> None:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=OUTPUT_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(winners[state].as_row() for state in sorted(winners))


def write_reference(paths: list[Path], output: Path) -> dict[str, int]:
    """Generate reference output for callers such as the pipeline CLI."""
    winners = compute_winners(paths)
    write_results(winners, output)
    return {"states": len(winners)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, action="append", nargs="+", type=Path,
                        help="Source CSV(s); this option can be repeated")
    parser.add_argument("--output", required=True, type=Path,
                        help="Output CSV with one deterministic winner per state")
    args = parser.parse_args()
    paths = [path for group in args.csv for path in group]
    try:
        winners = compute_winners(paths)
        write_results(winners, args.output)
    except (OSError, ValueError, csv.Error) as error:
        parser.exit(1, f"error: {error}\n")
    print(f"Wrote {len(winners)} state winners to {args.output} (CSV reference oracle)")


if __name__ == "__main__":
    main()
