"""Exercise the actual Lab 5/6 code with deterministic Wikipedia events."""
import json
from datetime import datetime

from pyspark.sql import functions as F

from lab5.contracts import WIKI_RULES
from lab5.fixtures import wiki_fixture_rows
from lab5.transforms import annotate_quality, parse_wiki
from lab6.transforms import aggregate_activity, prepare_edits


def test_quality_routing_accounts_for_every_bronze_record(spark):
    schema = (
        "event_json STRING, kafka_topic STRING, kafka_partition INT, "
        "kafka_offset BIGINT, kafka_enqueued_at TIMESTAMP, _ingested_at TIMESTAMP"
    )
    bronze = spark.createDataFrame(wiki_fixture_rows(), schema)
    rows = annotate_quality(parse_wiki(bronze), WIKI_RULES).collect()
    valid = [row for row in rows if row._is_valid]
    quarantine = [row for row in rows if not row._is_valid]
    assert len(rows) == 7
    assert len(valid) == 1
    assert len(quarantine) == 6
    assert len(valid) + len(quarantine) == bronze.count()
    assert valid[0].event_time == datetime(2023, 11, 14, 22, 13, 20)
    expected_errors = {
        1: "valid_title", 2: "valid_user", 3: "valid_timestamp",
        4: "valid_lengths", 5: "valid_domain", 6: "valid_json",
    }
    for row in quarantine:
        assert expected_errors[row.kafka_offset] in row._quality_errors


def test_parser_fallback_id_and_malformed_payloads(spark):
    event = {
        "id": 42, "wiki": "enwiki", "type": "edit", "title": " Page ",
        "user": " User ", "timestamp": 1700000000,
        "meta": {"domain": "en.wikipedia.org"},
    }
    source = spark.createDataFrame(
        [(json.dumps(event),), ("null",), ("{}",), ("{broken-json",)],
        "event_json STRING",
    )
    rows = annotate_quality(parse_wiki(source), WIKI_RULES).collect()
    assert rows[0]._is_valid
    assert (rows[0].event_id, rows[0].title, rows[0].user_name) == ("enwiki:42", "Page", "User")
    assert all(not row._is_valid for row in rows[1:])
    assert "valid_json" in rows[1]._quality_errors
    assert "valid_event_id" in rows[2]._quality_errors
    assert "valid_json" in rows[3]._quality_errors


def edit_fixture(spark):
    schema = (
        "event_id STRING, event_time STRING, wiki STRING, title STRING, "
        "user_name STRING, namespace INT, bot BOOLEAN, old_length LONG, "
        "new_length LONG, old_revision LONG, new_revision LONG, "
        "kafka_enqueued_at STRING, kafka_topic STRING, kafka_partition INT, kafka_offset LONG"
    )
    rows = [
        ("a", "2026-10-04 15:04:59", "enwiki", "Same", "Same", 0, False, 100, 150, 1, 2, "2026-10-04 15:05:00", "events", 0, 1),
        ("a", "2026-10-04 15:04:59", "enwiki", "Same", "Same", 0, False, 100, 999, 1, 2, "2026-10-04 15:05:01", "events", 0, 2),
        ("b", "2026-10-04 15:05:00", "plwiki", "Same", "Same", 0, True, 150, 100, 3, 4, "2026-10-04 15:05:01", "events", 0, 3),
        ("c", "2026-10-04 15:05:10", "plwiki", "Other", "Same", 1, False, None, 120, 4, 5, "2026-10-04 15:05:11", "events", 0, 4),
    ]
    return spark.createDataFrame(rows, schema).withColumn("event_time", F.to_timestamp("event_time"))


def test_gold_deduplication_scoped_keys_and_utc_buckets(spark):
    rows = {row.event_id: row for row in prepare_edits(edit_fixture(spark)).collect()}
    assert set(rows) == {"a", "b", "c"}
    assert rows["a"].new_length == 150  # Keep the earliest delivery.
    for key in ("wiki_key", "page_key", "editor_key", "namespace_key"):
        assert rows["a"][key] != rows["b"][key]
    assert rows["b"].editor_key == rows["c"].editor_key
    assert rows["a"].bucket_start == datetime(2026, 10, 4, 15, 0)
    assert rows["b"].bucket_start == datetime(2026, 10, 4, 15, 5)
    assert rows["a"].date_key == 20261004


def test_gold_measures_and_aggregate_reconciliation(spark):
    fact = prepare_edits(edit_fixture(spark))
    rows = {row.event_id: row for row in fact.collect()}
    assert (rows["a"].bytes_delta, rows["a"].bytes_added, rows["a"].bytes_removed) == (50, 50, 0)
    assert (rows["b"].bytes_delta, rows["b"].bytes_added, rows["b"].bytes_removed) == (-50, 0, 50)
    assert rows["c"].bytes_delta is None
    assert rows["c"].bytes_added is None
    assert rows["c"].bytes_removed is None
    aggregates = {row.wiki: row for row in aggregate_activity(fact, ["wiki"]).collect()}
    assert sum(row.edit_count for row in aggregates.values()) == 3
    assert sum(row.net_bytes_delta for row in aggregates.values()) == 0
    polish = aggregates["plwiki"]
    assert (polish.edit_count, polish.editor_count, polish.page_count) == (2, 1, 2)
    assert polish.bot_share_pct == 50
    assert polish.edits_with_lengths == 1
