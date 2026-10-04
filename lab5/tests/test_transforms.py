"""Local Spark tests. No Databricks access needed."""
import json
import os
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pyspark.sql import SparkSession
from lab5.contracts import TITLE_COLUMNS, TITLE_RULES, WIKI_RULES
from lab5.fixtures import wiki_fixture_rows
from lab5.transforms import annotate_quality, clean_titles, kafka_options, parse_wiki, read_titles


class TransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        cls.spark = (
            SparkSession.builder.master("local[2]").appName("lab5-local-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_original_csv_and_nullable_attributes(self):
        path = str(Path(__file__).resolve().parents[2] / "dataset_example/netflix_titles.csv")
        df = annotate_quality(clean_titles(read_titles(self.spark, path)), TITLE_RULES).cache()
        try:
            self.assertEqual(df.count(), 8807)
            self.assertEqual(df.filter("NOT _is_valid").count(), 0)
            self.assertEqual(df.select("show_id").distinct().count(), 8807)
            self.assertGreater(df.filter("director IS NULL AND _is_valid").count(), 0)
            self.assertEqual(df.filter("date_added IS NULL").count(), 10)
        finally:
            df.unpersist()

    def test_invalid_title_values_under_ansi(self):
        rows = []
        for values in [
            {"show_id": "s1", "title": " Valid ", "type": "Movie", "release_year": "2020", "date_added": "invalid-date"},
            {"show_id": "s2", "title": "  ", "type": "Movie", "release_year": "invalid-year"},
        ]:
            rows.append(tuple(values.get(name) for name in TITLE_COLUMNS) + (None,))
        schema = ", ".join(f"`{name}` STRING" for name in TITLE_COLUMNS) + ", _corrupt_record STRING"
        df = annotate_quality(clean_titles(self.spark.createDataFrame(rows, schema)), TITLE_RULES)
        results = {row.show_id: row for row in df.collect()}
        self.assertTrue(results["s1"]._is_valid)
        self.assertEqual(results["s1"].title, "Valid")
        self.assertIsNone(results["s1"].date_added)
        self.assertEqual(set(results["s2"]._quality_errors), {"valid_title", "valid_release_year"})

    def test_wiki_routing_and_reasons(self):
        schema = "event_json STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_enqueued_at TIMESTAMP, _ingested_at TIMESTAMP"
        df = annotate_quality(parse_wiki(self.spark.createDataFrame(wiki_fixture_rows(), schema)), WIKI_RULES)
        rows = {row.kafka_offset: row for row in df.collect()}
        self.assertTrue(rows[0]._is_valid)
        self.assertEqual(sum(row._is_valid for row in rows.values()), 1)
        for offset, rule in [(1, "valid_title"), (2, "valid_user"), (3, "valid_timestamp"), (4, "valid_lengths"), (5, "valid_domain"), (6, "valid_json")]:
            self.assertIn(rule, rows[offset]._quality_errors)
        self.assertEqual(rows[0].event_time.year, 2023)

    def test_wiki_null_and_bad_typed_json(self):
        base = {"meta": {"id": "e1", "domain": "en.wikipedia.org"}, "type": "edit", "title": "Page", "user": "User", "wiki": "enwiki", "timestamp": "not-a-number"}
        df = self.spark.createDataFrame([(json.dumps(base),), ("null",), ("{}",)], "event_json STRING")
        rows = annotate_quality(parse_wiki(df), WIKI_RULES).collect()
        self.assertTrue(all(not row._is_valid for row in rows))
        self.assertIn("valid_timestamp", rows[0]._quality_errors)
        self.assertIn("valid_json", rows[1]._quality_errors)

    def test_jaas_escaping_and_retention_safety(self):
        options = kafka_options("namespace", "hub", 'secret"with\\escapes')
        self.assertIn('password="secret\\"with\\\\escapes";', options["kafka.sasl.jaas.config"])
        self.assertEqual(options["failOnDataLoss"], "true")
        self.assertNotIn("kafka.group.id", options)


if __name__ == "__main__":
    unittest.main(verbosity=2)
