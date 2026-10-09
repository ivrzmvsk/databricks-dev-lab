"""Exercise the same check groups the headless gate calls, including failed gates."""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from lab7.checks import (
    build_summary,
    check_aggregates,
    check_dimensions,
    check_dqx,
    check_layers,
    check_routing,
    check_timeliness,
    record,
)
from lab7.constraints import BRONZE_SCHEMA
from lab7.fixtures import demo_rows
from lab7.quality import (
    classify,
    gate,
    make_aggregates,
    make_dimensions,
    make_fact,
    rank_deliveries,
)


@pytest.fixture
def medallion(spark):
    bronze = spark.createDataFrame(demo_rows(), BRONZE_SCHEMA)
    classified = classify(bronze)
    accepted = rank_deliveries(classified.filter("_is_valid"))
    silver = accepted.filter("_delivery = 1")
    fact = make_fact(silver)
    frames = {
        "bronze": bronze,
        "silver": silver,
        "quarantine": classified.filter("NOT _is_valid"),
        "duplicates": accepted.filter("_delivery > 1"),
        "fact": fact,
    }
    return frames, accepted, make_dimensions(fact), make_aggregates(fact)


def by_name(results):
    return {r["check"]: r for r in results}


def test_healthy_medallion_check_groups(medallion):
    frames, _, dimensions, aggregates = medallion
    counts = {name: frame.count() for name, frame in frames.items()}
    results = [
        *check_layers(frames, counts, {"real_rows": 9}),
        *check_routing(frames),
        *check_dimensions(frames["fact"], dimensions),
        *check_aggregates(frames["fact"], aggregates),
    ]
    assert len(results) == 47
    assert all(r["passed"] for r in results)
    gate(results)


def test_layer_gate_detects_snapshot_loss_and_duplicate_fact(medallion):
    frames, _, _, _ = medallion
    frames = {**frames, "fact": frames["fact"].unionByName(frames["fact"])}
    counts = {name: frame.count() for name, frame in frames.items()}
    results = check_layers(frames, counts, {"real_rows": 10})
    checks = by_name(results)
    assert not checks["snapshot_reconciliation"]["passed"]
    assert not checks["silver_to_fact"]["passed"]
    assert checks["fact_key_uniqueness"]["observed"] == 2
    with pytest.raises(AssertionError, match="fact_key_uniqueness"):
        gate(results)


def test_routing_group_detects_loss_despite_equal_counts(medallion):
    frames, _, _, _ = medallion
    first = frames["silver"].orderBy("kafka_offset").limit(1)
    frames = {**frames, "silver": first.unionByName(first)}
    results = check_routing(frames)
    checks = by_name(results)
    assert checks["routing_missing_rows"]["observed"] == 1
    assert checks["routing_extra_rows"]["observed"] == 1
    with pytest.raises(AssertionError, match="routing_missing_rows"):
        gate(results)


def test_routing_group_detects_missing_reasons_and_valid_leak(medallion):
    frames, _, _, _ = medallion
    bad_quarantine = (
        frames["quarantine"]
        .withColumn("_quality_errors", F.lit(None).cast("array<string>"))
        .withColumn("_is_valid", F.lit(True))
    )
    results = check_routing({**frames, "quarantine": bad_quarantine})
    checks = by_name(results)
    assert checks["quarantine_reasons"]["observed"] == 6
    assert checks["quarantine_valid_leak"]["observed"] == 6
    with pytest.raises(AssertionError, match="quarantine_reasons"):
        gate(results)


def test_dimension_group_detects_orphans_duplicates_and_nulls(medallion):
    frames, _, dimensions, _ = medallion
    wiki = dimensions["dim_wiki"]
    dimensions = {
        **dimensions,
        "dim_page": dimensions["dim_page"].limit(0),
        "dim_wiki": wiki.unionByName(wiki).unionByName(
            wiki.limit(1).withColumn("wiki_key", F.lit(None).cast("string"))
        ),
    }
    results = check_dimensions(frames["fact"], dimensions)
    checks = by_name(results)
    assert checks["dim_page_orphans"]["observed"] == 2
    assert checks["dim_wiki_uniqueness"]["observed"] > 0
    assert checks["dim_wiki_null_keys"]["observed"] == 1
    with pytest.raises(AssertionError, match="dim_page_orphans"):
        gate(results)


@pytest.mark.parametrize("corruption", ["edit_count", "net_bytes_delta", "empty"])
def test_aggregate_group_rejects_corruption(medallion, corruption):
    frames, _, _, aggregates = medallion
    aggregate = aggregates["agg_wiki_daily"]
    damaged = (
        aggregate.limit(0)
        if corruption == "empty"
        else aggregate.withColumn(corruption, F.col(corruption) + 1)
    )
    results = check_aggregates(frames["fact"], {"agg_wiki_daily": damaged})
    checks = by_name(results)
    measure = "edit_count" if corruption == "empty" else corruption
    assert not checks["agg_wiki_daily_" + measure]["passed"]
    with pytest.raises(AssertionError, match="agg_wiki_daily"):
        gate(results)


def test_dqx_group_preserves_source_routing_and_blocks_corrupt_facts(medallion, dqx_engine):
    frames, _, dimensions, _ = medallion
    counts = {name: frame.count() for name, frame in frames.items()}
    healthy, rejected, source_failures = check_dqx(frames, counts, dimensions, dqx_engine)
    assert len(healthy) == 11
    assert all(r["passed"] for r in healthy)
    assert rejected.count() == 0
    assert source_failures and counts["quarantine"] == 6
    broken = (
        frames["fact"].withColumn("edit_count", F.lit(-1)).withColumn("wiki_key", F.lit("unknown"))
    )
    results, rejected, _ = check_dqx({**frames, "fact": broken}, counts, dimensions, dqx_engine)
    checks = by_name(results)
    assert checks["dqx_fact_one_edit"]["observed"] == 2
    assert checks["dqx_fact_dim_wiki_reference"]["observed"] == 2
    assert rejected.count() == 2
    with pytest.raises(AssertionError, match="dqx_fact_one_edit"):
        gate(results)


def test_timeliness_group_enforces_outliers_and_optional_snapshot_sla(spark):
    now = datetime(2026, 10, 9, 12)
    rows = spark.createDataFrame(
        [(now, now), (now - timedelta(seconds=3601), now)],
        "event_time TIMESTAMP, _ingested_at TIMESTAMP",
    )
    manifest = {"measured_at": (now - timedelta(hours=2)).isoformat()}
    results, timing = check_timeliness(rows, rows, manifest, now, 3600)
    checks = by_name(results)
    assert timing["deliveries"] == 2 and timing["violations"] == 1
    assert not checks["snapshot_processing_freshness"]["passed"]
    with pytest.raises(AssertionError, match="delivery_delay_sla"):
        gate(results)
    healthy = rows.filter("event_time = _ingested_at")
    manual, _ = check_timeliness(healthy, healthy, manifest, now)
    assert by_name(manual)["snapshot_processing_freshness"]["severity"] == "warning"
    gate(manual)


@pytest.mark.parametrize("mandatory_failure", [False, True])
def test_summary_and_gate_agree_on_severity(mandatory_failure):
    counts = {"bronze": 9, "silver": 2, "quarantine": 6, "duplicates": 1, "fact": 2}
    results = [record("historical", "timeliness", 0, 1, severity="warning")]
    if mandatory_failure:
        results.append(record("lost_rows", "consistency", 1))
    summary = build_summary("snapshot", counts, results, {"invalid": 6}, {}, [])
    assert summary["passed"] is not mandatory_failure
    assert len(summary["warnings"]) == 1
    assert len(summary["failed_checks"]) == int(mandatory_failure)
    assert summary["real_data_quality"]["accepted_deliveries"] == 3
    assert summary["real_data_quality"]["quarantine_rate"] == 6 / 9
    if mandatory_failure:
        with pytest.raises(AssertionError, match="lost_rows"):
            gate(results)
    else:
        gate(results)


def test_empty_input_summary_does_not_divide_by_zero():
    counts = dict.fromkeys(("bronze", "silver", "quarantine", "duplicates", "fact"), 0)
    summary = build_summary(
        "empty", counts, [record("bronze_rows_present", "completeness", 0, 1)], {}, {}, []
    )
    assert summary["real_data_quality"]["quarantine_rate"] is None
    assert not summary["passed"]


@pytest.mark.parametrize("case_index", range(4))
def test_negative_write_cases_violate_their_constraint(medallion, case_index):
    from lab7.constraints import FACT_CONSTRAINTS, NEGATIVE_WRITES

    frames, _, _, _ = medallion
    _, expression, column = NEGATIVE_WRITES[case_index]
    healthy = frames["fact"].limit(1)
    damaged = healthy.withColumn(column, F.expr(expression))
    if column == "event_id":
        assert healthy.filter("event_id IS NULL").count() == 0
        assert damaged.filter("event_id IS NULL").count() == 1
    else:
        constraint = {
            "edit_count": "one_edit",
            "old_length": "lengths_nonnegative",
            "bytes_delta": "bytes_consistent",
        }[column]
        predicate = FACT_CONSTRAINTS[constraint]
        assert healthy.filter(f"NOT ({predicate})").count() == 0
        assert damaged.filter(f"NOT ({predicate})").count() == 1
