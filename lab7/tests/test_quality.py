from lab7.constraints import BRONZE_SCHEMA
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from lab7.config import Config
from lab7.quality import (
    classify,
    rank_deliveries,
    evaluate_rows,
    reconcile_deliveries,
    ROW_RULES,
    FACT_RULES,
    freshness_check,
    delivery_timeliness,
    snapshot_age_check,
    gate,
    enrich_wikis,
)
from lab7.fixtures import demo_rows


def test_classification_deduplication_and_branch_reconciliation(spark):
    schema = BRONZE_SCHEMA
    rows = spark.createDataFrame(demo_rows(), schema)
    classified = classify(rows)
    accepted = rank_deliveries(classified.filter("_is_valid"))
    assert classified.filter("NOT _is_valid").count() == 6
    assert accepted.filter("_delivery = 1").count() == 2
    assert accepted.filter("_delivery > 1").count() == 1
    assert all(n == 0 for n in evaluate_rows(accepted, ROW_RULES).values())


def test_null_predicate_is_a_quality_failure(spark):
    rows = spark.createDataFrame([(None,), (0,), (1,)], "value INT")
    assert evaluate_rows(rows, {"positive": "value > 0"}) == {"positive": 2}


def test_reference_join_preserves_unknown_wikis(spark):
    wikis = spark.createDataFrame(
        [("enwiki", "key-en"), ("plwiki", "key-pl")], "wiki STRING, wiki_key STRING"
    )
    refs = spark.createDataFrame(
        [("enwiki", "https://en.wikipedia.org")], "wiki STRING, site_url STRING"
    )
    rows = {r.wiki: r for r in enrich_wikis(wikis, refs).collect()}
    assert set(rows) == {"enwiki", "plwiki"}
    assert rows["enwiki"].metadata_matched
    assert not rows["plwiki"].metadata_matched
    assert rows["plwiki"].wiki_key == "key-pl"


def test_bytes_consistency_detects_corrupted_measures(spark):
    data = spark.createDataFrame(
        [(50, 50, 0), (50, 10, 0), (None, None, None)],
        "bytes_delta LONG, bytes_added LONG, bytes_removed LONG",
    )
    assert evaluate_rows(data, {"bytes_consistent": FACT_RULES["bytes_consistent"]}) == {
        "bytes_consistent": 1
    }


def test_equal_counts_cannot_hide_a_lost_delivery(spark):
    bronze = spark.createDataFrame([(1,), (2,)], "offset LONG")
    bad_silver = spark.createDataFrame([(1,), (1,)], "offset LONG")
    checks = reconcile_deliveries(bronze, [bad_silver], ["offset"])
    assert checks == {"missing_rows": 1, "extra_rows": 1}
    with pytest.raises(AssertionError, match="missing_rows"):
        gate(
            [
                {"check": name, "severity": "error", "passed": count == 0}
                for name, count in checks.items()
            ]
        )


def test_valid_and_quarantine_deliveries_reconcile(spark):
    bronze = spark.createDataFrame([(1,), (2,)], "offset LONG")
    silver = spark.createDataFrame([(1,)], "offset LONG")
    quarantine = spark.createDataFrame([(2,)], "offset LONG")
    assert reconcile_deliveries(bronze, [silver, quarantine], ["offset"]) == {
        "missing_rows": 0,
        "extra_rows": 0,
    }


@pytest.mark.parametrize("age,passed", [(0, True), (3600, True), (3601, False), (-1, False)])
def test_freshness_boundaries(age, passed):
    reference = datetime(2026, 10, 4, 15)
    assert freshness_check(reference - timedelta(seconds=age), reference)["passed"] is passed


def test_empty_timestamp_is_not_fresh():
    assert not freshness_check(None, datetime(2026, 10, 4))["passed"]
    assert not freshness_check(datetime(2026, 10, 4), None)["passed"]


def test_manual_snapshot_age_is_measured_and_job_sla_is_enforced():
    now = datetime(2026, 10, 5, 12)
    captured = now - timedelta(hours=2)
    manual = snapshot_age_check(captured, now)
    assert manual["age_seconds"] == 7200 and manual["severity"] == "warning"
    gate([{"check": "snapshot_age", **manual}])
    job = snapshot_age_check(captured, now, 3600)
    assert job["severity"] == "error" and not job["passed"]
    with pytest.raises(AssertionError, match="snapshot_age"):
        gate([{"check": "snapshot_age", **job}])
    assert snapshot_age_check(captured, now, 10800)["passed"]
    with pytest.raises(ValueError):
        snapshot_age_check(captured, now, 0)


def test_fresh_maxima_cannot_hide_late_or_future_deliveries(spark):
    reference = datetime(2026, 10, 5, 12)
    rows = spark.createDataFrame(
        [
            ("fresh", reference, reference),
            ("late", reference - timedelta(seconds=3601), reference),
            ("future", reference + timedelta(seconds=1), reference),
            ("missing", None, reference),
            ("boundary", reference - timedelta(seconds=3600), reference),
        ],
        "event_id STRING, event_time TIMESTAMP, _ingested_at TIMESTAMP",
    )
    result = {row.event_id: row._delivery_on_time for row in delivery_timeliness(rows).collect()}
    assert result == {
        "fresh": True,
        "late": False,
        "future": False,
        "missing": False,
        "boundary": True,
    }
    maxima = (
        rows.filter("event_id IN ('fresh', 'late')")
        .agg(F.max("event_time").alias("event"), F.max("_ingested_at").alias("ingestion"))
        .first()
    )
    assert freshness_check(maxima.event, maxima.ingestion)["passed"]


def test_gate_rejects_errors_but_allows_measured_warnings():
    gate([{"check": "snapshot_age", "severity": "warning", "passed": False}])
    with pytest.raises(AssertionError, match="missing_rows"):
        gate([{"check": "missing_rows", "severity": "error", "passed": False}])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"schema": "ivanrazumovskyi_lab5"},
        {"schema": "oops; DROP SCHEMA x"},
        {"catalog": "bad-name"},
    ],
)
def test_output_isolation(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)
