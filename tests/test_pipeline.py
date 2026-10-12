"""Check CSV result equivalence and HTTP transport without external services."""
import base64
import contextlib
from decimal import Decimal
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from app.clickhouse import ClickHouse, read_query
from app.pipeline import main, normalized_csv
from app.producer import preflight


HEADER = "us_state,cat_id,max_amount,transaction_id\n"


class CSVComparisonTests(unittest.TestCase):
    def test_equivalence_ignores_row_order_and_money_display_precision(self):
        raw = HEADER + "TX,fuel,12.30,tx-id\nCA,food,0.29,ca-id\n"
        aggregate = HEADER + "CA,food,0.2900,ca-id\nTX,fuel,12.3,tx-id\n"
        self.assertEqual(normalized_csv(raw), normalized_csv(aggregate))
        self.assertEqual(normalized_csv(raw), [
            ("CA", "food", Decimal("0.29"), "ca-id"),
            ("TX", "fuel", Decimal("12.30"), "tx-id"),
        ])

    def test_equivalence_compares_category_and_identity_as_well_as_money(self):
        baseline = HEADER + "CA,food,1.00,first\n"
        for changed in ["CA,fuel,1.00,first\n", "CA,food,1.00,second\n",
                        "CA,food,1.01,first\n", "TX,food,1.00,first\n"]:
            with self.subTest(row=changed):
                self.assertNotEqual(normalized_csv(baseline), normalized_csv(HEADER + changed))

    def test_equivalence_keeps_duplicate_rows_visible(self):
        row = "CA,food,1.00,first\n"
        self.assertNotEqual(normalized_csv(HEADER + row), normalized_csv(HEADER + row + row))

    def test_quoted_categories_and_maximum_decimal_remain_exact(self):
        actual = normalized_csv(HEADER + 'CA,"food, drink",9999999999999999.99,id\n')
        self.assertEqual(actual, [("CA", "food, drink", Decimal("9999999999999999.99"), "id")])

    def test_wrong_output_columns_raise_instead_of_claiming_equivalence(self):
        with self.assertRaises(KeyError):
            normalized_csv("us_state\tcat_id\tmax_amount\ttransaction_id\nCA\tfood\t1\tid\n")


class HTTPResponse:
    def __init__(self, body, summary=None):
        self.body = body
        self.headers = {} if summary is None else {"X-ClickHouse-Summary": summary}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.body


class ClickHouseClientTests(unittest.TestCase):
    def test_sends_sql_auth_and_urlencoded_settings_and_reads_summary(self):
        environment = {"CLICKHOUSE_URL": "http://example.test:8123/",
                       "CLICKHOUSE_USER": "user", "CLICKHOUSE_PASSWORD": "secret"}
        with patch.dict(os.environ, environment), patch("app.clickhouse.urlopen") as fetch:
            fetch.return_value = HTTPResponse(b"CA,food,0.29,id\n", '{"read_rows":"17"}')
            result = ClickHouse().query("SELECT 'café'", query_id="query with spaces", default_format="CSVWithNames")
        request = fetch.call_args.args[0]
        self.assertEqual(request.data.decode(), "SELECT 'café'")
        self.assertEqual(request.get_header("Authorization"),
                         "Basic " + base64.b64encode(b"user:secret").decode())
        self.assertEqual(parse_qs(urlsplit(request.full_url).query),
                         {"query_id": ["query with spaces"], "default_format": ["CSVWithNames"]})
        self.assertEqual(result, ("CA,food,0.29,id\n", {"read_rows": "17"}))

    def test_missing_summary_is_allowed(self):
        with patch("app.clickhouse.urlopen", return_value=HTTPResponse(b"1\n")):
            self.assertEqual(ClickHouse().query("SELECT 1"), ("1\n", {}))

    def test_server_error_contains_status_and_clickhouse_diagnostic(self):
        error = HTTPError("http://example.test", 500, "Error", {}, io.BytesIO(b"Unknown table"))
        with patch("app.clickhouse.urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "ClickHouse HTTP 500: Unknown table"):
                ClickHouse().query("SELECT * FROM missing")

    def test_executes_project_style_sql_statements_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "schema.sql"
            path.write_text("-- comment\nCREATE DATABASE test;\n"
                            "CREATE TABLE test.x (x UInt64) ENGINE=Memory; -- trailing comment\n")
            client = ClickHouse()
            with patch.object(client, "query") as query:
                client.execute_file(path)
            self.assertEqual([c.args[0].strip() for c in query.call_args_list], [
                "CREATE DATABASE test", "CREATE TABLE test.x (x UInt64) ENGINE=Memory",
            ])

    def test_query_reader_removes_terminal_semicolon_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "query.sql"
            path.write_text("SELECT 'literal;value';\n")
            self.assertEqual(read_query(path), "SELECT 'literal;value'")


class PipelineFlowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "input.csv"
        self.source.write_text("transaction_time,us_state,cat_id,amount\n"
                               "2025-01-02 12:34,CA,food,0.29\n")
        self.output = self.root / "results"
        identity = hashlib.sha256(b"input.csv:1").hexdigest()
        self.expected_csv = HEADER + f"CA,food,0.29,{identity}\n"
        self.base = self.expected_csv
        self.aggregate = self.expected_csv
        self.stored = 0
        self.unique = 1
        self.physical = 1
        self.client = Mock(spec=ClickHouse)
        self.client.query.side_effect = self.query
        self.published = dict(preflight([self.source]), acknowledged_rows=1,
                              topic="transactions", elapsed_seconds=0.01)

    def query(self, sql, **settings):
        if sql == "SELECT count() FROM homework.transactions":
            return str(self.stored), {}
        if "uniqExact(transaction_id)" in sql:
            # ClickHouse quotes UInt64 values in JSON by default.
            return json.dumps({"count()": str(self.physical),
                               "uniqExact(transaction_id)": str(self.unique)}), {}
        if "argMaxMerge" in sql:
            self.assertEqual(settings.get("default_format"), "CSVWithNames")
            return self.aggregate, {}
        if "argMax(" in sql:
            self.assertEqual(settings.get("default_format"), "CSVWithNames")
            return self.base, {}
        self.fail(f"Unexpected query: {sql}")

    @contextlib.contextmanager
    def running_pipeline(self):
        args = ["pipeline", "--csv", str(self.source), "--output-dir", str(self.output),
                "--timeout", "0"]
        with patch.object(sys, "argv", args), \
                patch("app.pipeline.ClickHouse", return_value=self.client) as factory, \
                patch("app.pipeline.publish", return_value=self.published) as publisher, \
                contextlib.redirect_stdout(io.StringIO()):
            yield factory, publisher

    def test_success_crosschecks_reference_and_exports_verified_manifest(self):
        self.physical = 2  # At-least-once storage duplicates do not change the winner.
        with self.running_pipeline() as (_, publisher):
            main()
        publisher.assert_called_once()
        self.assertEqual((self.output / "largest_transaction_by_state.csv").read_text(), self.expected_csv)
        manifest = json.loads((self.output / "run_manifest.json").read_text())
        self.assertEqual(manifest["clickhouse_unique_rows"], 1)
        self.assertEqual(manifest["clickhouse_physical_rows"], 2)
        self.assertEqual(manifest["state_count"], 1)
        self.assertTrue(manifest["raw_optimized_reference_match"])
        self.assertIn("ClickHouse SQL", manifest["result_origin"])

    def test_invalid_source_is_rejected_before_database_or_kafka_operations(self):
        self.source.write_text("wrong,headers\ninvalid,row\n")
        with self.running_pipeline() as (factory, publisher):
            with self.assertRaises(ValueError):
                main()
            factory.assert_not_called()
            publisher.assert_not_called()

    def test_existing_data_prevents_publishing(self):
        self.stored = 1
        with self.running_pipeline() as (_, publisher):
            with self.assertRaisesRegex(RuntimeError, "not empty"):
                main()
            publisher.assert_not_called()
        self.assertFalse((self.output / "run_manifest.json").exists())

    def test_timed_out_ingestion_does_not_claim_success(self):
        self.unique = self.physical = 0
        with self.running_pipeline():
            with self.assertRaisesRegex(TimeoutError, "ingestion timed out"):
                main()
        self.assertFalse((self.output / "run_manifest.json").exists())

    def test_extra_source_identities_are_rejected(self):
        self.unique = self.physical = 2
        with self.running_pipeline():
            with self.assertRaisesRegex(RuntimeError, "Unexpected extra rows"):
                main()

    def test_aggregate_disagreement_does_not_export_answer(self):
        self.aggregate = self.aggregate.replace("food", "wrong-category")
        with self.running_pipeline():
            with self.assertRaisesRegex(AssertionError, "aggregate-table queries disagree"):
                main()
        self.assertFalse((self.output / "largest_transaction_by_state.csv").exists())

    def test_agreeing_database_queries_must_also_match_independent_reference(self):
        self.base = self.aggregate = self.expected_csv.replace("food", "wrong-category")
        with self.running_pipeline():
            with self.assertRaisesRegex(AssertionError, "independent CSV reference"):
                main()
        self.assertFalse((self.output / "largest_transaction_by_state.csv").exists())


if __name__ == "__main__":
    unittest.main()
