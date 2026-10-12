"""Validate source data and Kafka delivery behavior without a running broker."""
import contextlib
import csv
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from app.producer import iter_records, normalize, preflight, publish


FIELDS = ["transaction_time", "us_state", "cat_id", "amount"]


class SourceFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write_csv(self, name="input.csv", rows=None, fields=FIELDS):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(fields)
            writer.writerows(rows if rows is not None else [
                ["2025-01-02 12:34", "CA", "grocery", "0.29"],
                ["2025-01-02 12:35:07", "TX", "fuel", "1234.56"],
            ])
        return path


class NormalizationTests(unittest.TestCase):
    def row(self, **changes):
        return dict(transaction_time="2025-01-02 12:34", us_state="CA",
                    cat_id="grocery", amount="0.29", **changes)

    def test_minute_timestamp_and_exact_money(self):
        actual = normalize(self.row(), "train.csv", 7)
        self.assertEqual(actual["transaction_time"], "2025-01-02 12:34:00")
        self.assertEqual(actual["amount"], "0.29")
        self.assertIsInstance(actual["amount"], str)
        self.assertEqual(actual["source_file"], "train.csv")
        self.assertEqual(actual["source_row"], 7)
        self.assertEqual(actual["transaction_id"],
                         hashlib.sha256(b"train.csv:7").hexdigest())

    def test_provenance_is_repeatable_and_distinguishes_files_and_rows(self):
        row = self.row()
        first = normalize(row, "train.csv", 1)
        self.assertEqual(first, normalize(dict(row), "train.csv", 1))
        self.assertNotEqual(first["transaction_id"], normalize(row, "test.csv", 1)["transaction_id"])
        self.assertNotEqual(first["transaction_id"], normalize(row, "train.csv", 2)["transaction_id"])

    def test_decimal_boundaries_and_trailing_zero_precision(self):
        for source, expected in [("0", "0.00"), ("0.2900", "0.29"),
                                 ("9999999999999999.99", "9999999999999999.99")]:
            with self.subTest(amount=source):
                row = self.row()
                row["amount"] = source
                self.assertEqual(normalize(row, "x.csv", 1)["amount"], expected)

    def test_rejects_nonfinite_negative_fractional_cent_and_out_of_range_amounts(self):
        for amount in ["NaN", "sNaN", "Infinity", "-Infinity", "-0.01", "0.001",
                       "1.234", "10000000000000000", "1e9999", "", "not-money"]:
            with self.subTest(amount=amount):
                row = self.row()
                row["amount"] = amount
                with self.assertRaises(ValueError):
                    normalize(row, "x.csv", 1)

    def test_rejects_bad_or_out_of_range_timestamp(self):
        for stamp in ["2025-02-30 12:00", "2025-01-01T12:00", "2025-01-01 12:00Z",
                      "2025-1-1 12:00", "1969-12-31 23:59:59", "2106-02-07 06:28:16"]:
            with self.subTest(timestamp=stamp):
                row = self.row()
                row["transaction_time"] = stamp
                with self.assertRaises(ValueError):
                    normalize(row, "x.csv", 1)

    def test_datetime_boundaries(self):
        for stamp in ["1970-01-01 00:00:00", "2106-02-07 06:28:15"]:
            row = self.row()
            row["transaction_time"] = stamp
            self.assertEqual(normalize(row, "x.csv", 1)["transaction_time"], stamp)

    def test_rejects_missing_required_fields_and_extra_csv_values(self):
        for field in FIELDS:
            with self.subTest(field=field):
                row = self.row()
                del row[field]
                with self.assertRaises(ValueError):
                    normalize(row, "x.csv", 1)
        row = self.row()
        row[None] = ["unexpected extra field"]
        with self.assertRaises(ValueError):
            normalize(row, "x.csv", 1)

    def test_rejects_empty_category_and_invalid_state(self):
        for key, value in [("cat_id", ""), ("cat_id", "  "), ("us_state", "ca"),
                           ("us_state", "CAL"), ("us_state", "1A")]:
            with self.subTest(key=key, value=value):
                row = self.row()
                row[key] = value
                with self.assertRaises(ValueError):
                    normalize(row, "x.csv", 1)


class CSVTests(SourceFixture):
    def test_target_column_is_optional_and_irrelevant_fields_can_contain_commas(self):
        source = self.write_csv(rows=[["2025-01-02 12:34", "CA", "grocery", "0.29",
                                      'merchant, with "quotes"', "first line\nsecond line"]],
                                fields=FIELDS + ["merchant", "address"])
        records = list(iter_records(source))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["amount"], "0.29")
        self.assertEqual(records[0]["source_row"], 1)
        self.assertNotIn("merchant", records[0])
        self.assertNotIn("target", records[0])

    def test_bom_is_supported_and_row_numbers_count_records_not_physical_lines(self):
        source = self.write_csv(rows=[
            ["2025-01-02 12:34", "CA", "grocery", "1", "first\nsecond"],
            ["2025-01-02 12:35", "TX", "fuel", "2", "third"],
        ], fields=FIELDS + ["note"])
        source.write_bytes(b"\xef\xbb\xbf" + source.read_bytes())
        records = list(iter_records(source))
        self.assertEqual([r["source_row"] for r in records], [1, 2])

    def test_rejects_duplicate_or_missing_headers(self):
        for fields in [FIELDS + ["amount"], FIELDS[:-1]]:
            with self.subTest(fields=fields):
                path = self.write_csv(fields=fields, rows=[])
                with self.assertRaises(ValueError):
                    list(iter_records(path))

    def test_rejects_incorrect_field_count_even_for_unused_fields(self):
        for rows, fields in [
            ([["2025-01-02 12:34", "CA", "grocery"]], FIELDS),
            ([["2025-01-02 12:34", "CA", "grocery", "1", "extra"]], FIELDS),
            ([["2025-01-02 12:34", "CA", "grocery", "1"]], FIELDS + ["unused"]),
        ]:
            with self.subTest(rows=rows, fields=fields):
                path = self.write_csv(rows=rows, fields=fields)
                with self.assertRaises(ValueError):
                    list(iter_records(path))

    def test_rejects_unclosed_csv_quote(self):
        path = self.root / "broken.csv"
        path.write_text("transaction_time,us_state,cat_id,amount,unused\n"
                        '2025-01-02 12:34,CA,grocery,1,"unterminated\n')
        with self.assertRaises((ValueError, csv.Error)):
            list(iter_records(path))

    def test_error_identifies_source_file_and_record_number(self):
        path = self.write_csv(rows=[
            ["2025-01-02 12:34", "CA", "grocery", "1"],
            ["2025-01-02 12:35", "CA", "grocery", "bad"],
        ])
        with self.assertRaisesRegex(ValueError, "input.csv.*data row 2"):
            list(iter_records(path))

    def test_preflight_counts_all_sources_and_hashes_original_bytes(self):
        first = self.write_csv("train.csv")
        second = self.write_csv("test.csv", rows=[["2025-01-02 12:34", "NY", "fuel", "2"]])
        result = preflight([first, second])
        self.assertEqual(result["expected_rows"], 3)
        self.assertEqual(result["sources"], [
            {"file": "train.csv", "rows": 2, "sha256": hashlib.sha256(first.read_bytes()).hexdigest()},
            {"file": "test.csv", "rows": 1, "sha256": hashlib.sha256(second.read_bytes()).hexdigest()},
        ])

    def test_preflight_rejects_empty_source(self):
        with self.assertRaisesRegex(ValueError, "no transactions"):
            preflight([self.write_csv(rows=[])])

    def test_preflight_rejects_duplicate_basenames_from_different_directories(self):
        first = self.write_csv("a/source.csv")
        second = self.write_csv("b/source.csv")
        with self.assertRaisesRegex(ValueError, "basenames"):
            preflight([first, second])


class FakeProducer:
    """Deterministic delivery callbacks, queue pressure and failure injection."""
    def __init__(self, config, *, backpressure=False, delivery_error=None,
                 acknowledge=True, pending=0, fail_on_poll=False):
        self.config = config
        self.backpressure = backpressure
        self.delivery_error = delivery_error
        self.acknowledge = acknowledge
        self.pending = pending
        self.fail_on_poll = fail_on_poll
        self.messages = []
        self.callbacks = []
        self.polls = []
        self.flush_timeout = None
        self.attempts = 0

    def produce(self, topic, *, key, value, on_delivery):
        self.attempts += 1
        if self.backpressure and self.attempts == 1:
            raise BufferError("local queue full")
        self.messages.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def poll(self, timeout):
        self.polls.append(timeout)
        if self.fail_on_poll and self.callbacks:
            self.callbacks.pop(0)("broker unavailable", None)

    def flush(self, timeout):
        self.flush_timeout = timeout
        if self.acknowledge:
            for callback in self.callbacks:
                callback(self.delivery_error, None)
            self.callbacks.clear()
        return self.pending


class PublishTests(SourceFixture):
    def fake_kafka(self, **options):
        instances = []

        def factory(config):
            instance = FakeProducer(config, **options)
            instances.append(instance)
            return instance

        self.addCleanup(patch.stopall)
        patch.dict(sys.modules, {"confluent_kafka": types.SimpleNamespace(Producer=factory)}).start()
        return instances

    def publish_quietly(self, paths, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return publish(paths, **kwargs)

    def test_all_inputs_are_validated_before_creating_producer(self):
        instances = self.fake_kafka()
        valid = self.write_csv("valid.csv")
        invalid = self.write_csv("invalid.csv", rows=[["2025-01-02 12:34", "CA", "food", "bad"]])
        with self.assertRaises(ValueError):
            self.publish_quietly([valid, invalid])
        self.assertEqual(instances, [])

    def test_requires_all_acknowledgments_and_uses_exact_payload_and_stable_key(self):
        instances = self.fake_kafka()
        source = self.write_csv()
        result = self.publish_quietly([source], bootstrap_servers="broker:19092", topic="test-topic")
        producer = instances[0]
        self.assertEqual(result["acknowledged_rows"], 2)
        self.assertEqual(result["expected_rows"], 2)
        self.assertEqual(result["topic"], "test-topic")
        self.assertTrue(producer.config["enable.idempotence"])
        self.assertEqual(producer.config["acks"], "all")
        self.assertEqual(producer.config["bootstrap.servers"], "broker:19092")
        self.assertEqual(len(producer.messages), 2)
        for (topic, key, payload), expected in zip(producer.messages, iter_records(source)):
            self.assertEqual(topic, "test-topic")
            self.assertEqual(json.loads(payload), expected)
            self.assertEqual(key.decode(), expected["transaction_id"])
        self.assertIsNotNone(producer.flush_timeout)

    def test_retries_queue_backpressure_without_losing_or_duplicating_records(self):
        instances = self.fake_kafka(backpressure=True)
        result = self.publish_quietly([self.write_csv()])
        self.assertEqual(result["acknowledged_rows"], 2)
        self.assertEqual(instances[0].attempts, 3)
        self.assertEqual(len(instances[0].messages), 2)
        self.assertIn(0.1, instances[0].polls)

    def test_delivery_error_from_flush_is_fatal(self):
        self.fake_kafka(delivery_error="not enough replicas")
        with self.assertRaisesRegex(RuntimeError, "Incomplete Kafka delivery.*not enough replicas"):
            self.publish_quietly([self.write_csv()])

    def test_delivery_error_during_poll_stops_further_produce_calls(self):
        instances = self.fake_kafka(fail_on_poll=True)
        with self.assertRaisesRegex(RuntimeError, "Kafka delivery failed.*broker unavailable"):
            self.publish_quietly([self.write_csv()])
        self.assertEqual(len(instances[0].messages), 1)

    def test_flush_pending_messages_are_fatal(self):
        self.fake_kafka(pending=1)
        with self.assertRaisesRegex(RuntimeError, "pending=1"):
            self.publish_quietly([self.write_csv()])

    def test_zero_pending_without_delivery_callbacks_is_not_success(self):
        self.fake_kafka(acknowledge=False, pending=0)
        with self.assertRaisesRegex(RuntimeError, "acknowledged=0"):
            self.publish_quietly([self.write_csv()])

    def test_manifest_row_count_mismatch_is_fatal(self):
        self.fake_kafka()
        source = self.write_csv()
        manifest = preflight([source])
        manifest["expected_rows"] += 1
        with self.assertRaisesRegex(RuntimeError, "Incomplete Kafka delivery"):
            self.publish_quietly([source], manifest=manifest)


if __name__ == "__main__":
    unittest.main()
