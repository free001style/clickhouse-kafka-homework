"""Tests of business semantics for the independent CSV reference oracle."""
import csv
from decimal import Decimal
import hashlib
from pathlib import Path
import tempfile
import unittest

from app.reference import compute_winners, transaction_id, write_results

ROOT = Path(__file__).resolve().parents[1]


class ReferenceTests(unittest.TestCase):
    def write_csv(self, directory, name, rows):
        path = Path(directory) / name
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(["us_state", "cat_id", "amount"])
            writer.writerows(rows)
        return path

    def test_demo_handles_ties_zeros_and_quoted_csv(self):
        actual = compute_winners([ROOT / "data" / "demo.csv"])
        self.assertEqual(set(actual), {"AZ", "CA", "NY", "TX", "WA"})
        self.assertEqual(actual["TX"].amount, Decimal("1234.56"))
        self.assertEqual(actual["NY"].amount, Decimal("0.00"))
        ny_candidates = [hashlib.sha256(f"demo.csv:{n}".encode()).hexdigest()
                         for n in range(9, 13)]
        self.assertEqual(actual["NY"].transaction_id, max(ny_candidates))
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "actual.csv"
            write_results(actual, output)
            self.assertEqual(output.read_bytes(), (ROOT / "results" / "demo_expected.csv").read_bytes())

    def test_combines_sources_with_exact_money_and_deterministic_ties(self):
        with tempfile.TemporaryDirectory() as directory:
            first = self.write_csv(directory, "first.csv", [("AZ", "low", "0.29"), ("TX", "A", "1.00")])
            second = self.write_csv(directory, "second.csv", [("AZ", "high", "0.30"), ("TX", "B", "1.00")])
            a = compute_winners([first, second])
            b = compute_winners([second, first])
            self.assertEqual(a, b)
            self.assertEqual(a["AZ"].cat_id, "high")
            self.assertEqual(a["AZ"].amount, Decimal("0.30"))
            self.assertEqual(a["TX"].transaction_id,
                             max(transaction_id("first.csv", 2), transaction_id("second.csv", 2)))

    def test_rejects_invalid_data_instead_of_silently_skipping_it(self):
        with tempfile.TemporaryDirectory() as directory:
            for amount in ["NaN", "Infinity", "-1", "0.001", "not-money"]:
                with self.subTest(amount=amount):
                    path = self.write_csv(directory, "invalid.csv", [("AZ", "food", amount)])
                    with self.assertRaises(ValueError):
                        compute_winners([path])

    def test_rejects_record_identity_collisions_from_duplicate_basenames(self):
        with tempfile.TemporaryDirectory() as directory:
            source = self.write_csv(directory, "input.csv", [("AZ", "food", "1.00")])
            with self.assertRaisesRegex(ValueError, "basenames"):
                compute_winners([source, source])


if __name__ == "__main__":
    unittest.main()
