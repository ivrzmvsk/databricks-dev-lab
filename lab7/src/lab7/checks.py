"""Testable medallion checks. Callers own table I/O, clocks and DQX clients."""

import json
from datetime import datetime

from pyspark.sql import functions as F

from lab7.dqx_checks import apply_trusted, failures_by_rule, predicate_checks, trusted_checks
from lab7.quality import (
    DEFAULT_MAX_AGE_SECONDS,
    DIMENSIONS,
    ROW_RULES,
    FACT_RULES,
    classify,
    evaluate_rows,
    reconcile_deliveries,
    freshness_check,
    delivery_timeliness,
    snapshot_age_check,
)
from lab7.source_validation import DIMENSION_KEYS


def record(name, dimension, observed, expected=0, severity="error", details=""):
    """Create one serializable equality check without changing report state."""
    return {
        "check": name,
        "dimension": dimension,
        "observed": float(observed),
        "expected": float(expected),
        "passed": observed == expected,
        "severity": severity,
        "details": str(details),
    }


def _report():
    results = []

    def append(*args, **kwargs):
        results.append(record(*args, **kwargs))

    return results, append


def check_layers(frames, counts, manifest):
    """Check snapshot accounting, layer counts, unique event keys and row rules."""
    results, add_check = _report()
    silver, fact = frames["silver"], frames["fact"]
    bronze_count, silver_count, quarantine_count, duplicate_count, fact_count = (
        counts[name] for name in ("bronze", "silver", "quarantine", "duplicates", "fact")
    )
    add_check("bronze_rows_present", "completeness", int(bronze_count > 0), 1)
    add_check(
        "branch_reconciliation",
        "consistency",
        silver_count + quarantine_count + duplicate_count,
        bronze_count,
    )
    add_check("snapshot_reconciliation", "consistency", bronze_count, manifest["real_rows"])
    add_check("silver_to_fact", "consistency", fact_count, silver_count)
    add_check(
        "silver_key_uniqueness",
        "uniqueness",
        silver_count - silver.select("event_id").distinct().count(),
    )
    add_check(
        "fact_key_uniqueness", "uniqueness", fact_count - fact.select("event_id").distinct().count()
    )
    for name, failures in evaluate_rows(silver, ROW_RULES).items():
        add_check("silver_" + name, DIMENSIONS[name], failures)
    for name, failures in evaluate_rows(fact, FACT_RULES).items():
        add_check("fact_" + name, "consistency", failures)
    return results


def check_dqx(frames, counts, reference_frames, dqx_engine):
    """Return checks, rejected fact rows and source violations using an injected engine."""
    results, add_check = _report()
    bronze, fact = frames["bronze"], frames["fact"]
    quarantine_count = counts["quarantine"]
    reference_keys = DIMENSION_KEYS
    trusted = apply_trusted(fact, FACT_RULES, reference_keys, reference_frames, dqx_engine)
    failed_dqx = failures_by_rule(trusted)
    for check in trusted_checks(FACT_RULES, reference_keys):
        name = check["name"]
        dimension = "uniqueness" if name == "event_id_unique" else "consistency"
        if name == "required_fact_keys":
            dimension = "completeness"
        add_check("dqx_fact_" + name, dimension, failed_dqx.get(name, 0))
    rejected = trusted.filter("_errors IS NOT NULL")
    add_check("dqx_fact_rejected_rows", "validity", rejected.count())
    source_checked = dqx_engine.apply_checks_by_metadata(
        classify(bronze).drop("_quality_errors", "_is_valid"),
        predicate_checks(ROW_RULES),
    )
    source_failures = failures_by_rule(source_checked)
    add_check(
        "dqx_source_routing",
        "consistency",
        source_checked.filter("_errors IS NOT NULL").count(),
        quarantine_count,
    )
    return results, rejected, source_failures


def check_routing(frames):
    """Check reject reasons and multiset conservation of Kafka deliveries."""
    results, add_check = _report()
    bronze, silver, quarantine, duplicates = (
        frames[name] for name in ("bronze", "silver", "quarantine", "duplicates")
    )
    add_check(
        "quarantine_reasons",
        "completeness",
        quarantine.filter("size(_quality_errors) = 0 OR _quality_errors IS NULL").count(),
    )
    add_check("quarantine_valid_leak", "consistency", quarantine.filter("_is_valid").count())
    # Branch counts alone could conceal one missing row and one extra row.
    delivery_key = ["kafka_topic", "kafka_partition", "kafka_offset"]
    for name, count in reconcile_deliveries(
        bronze, [silver, quarantine, duplicates], delivery_key
    ).items():
        add_check("routing_" + name, "consistency", count)
    return results


def check_dimensions(fact, dimensions):
    """Check unique, non-null dimension keys and references from every fact."""
    results, add_check = _report()
    for table, key in DIMENSION_KEYS.items():
        dimension = dimensions[table]
        add_check(
            table + "_uniqueness",
            "uniqueness",
            dimension.count() - dimension.select(key).distinct().count(),
        )
        add_check(
            table + "_null_keys", "completeness", dimension.filter(F.col(key).isNull()).count()
        )
        add_check(
            table + "_orphans",
            "consistency",
            fact.join(dimension.select(key), key, "left_anti").count(),
        )
    return results


def check_aggregates(fact, aggregates):
    """Compare count and byte totals; preserve the distinction between NULL and zero."""
    results, add_check = _report()
    metrics = ["edit_count", "bytes_added", "bytes_removed"]
    baseline = fact.agg(
        *[F.sum(c).alias(c) for c in metrics], F.sum("bytes_delta").alias("net_bytes_delta")
    ).first()
    for table, aggregate in aggregates.items():
        totals = aggregate.agg(*[F.sum(c).alias(c) for c in metrics + ["net_bytes_delta"]]).first()
        for measure in metrics + ["net_bytes_delta"]:
            # NULL totals and zero totals have different meanings.
            add_check(
                table + "_" + measure,
                "consistency",
                int(totals[measure] == baseline[measure]),
                1,
                details=f"aggregate={totals[measure]}, fact={baseline[measure]}",
            )
    return results


def check_timeliness(bronze, accepted, manifest, now, max_age_seconds=None):
    """Return checks and delay metrics using the caller's clock and optional Job SLA."""
    results, add_check = _report()
    timed = delivery_timeliness(accepted)
    timing = (
        timed.agg(
            F.count("*").alias("deliveries"),
            F.sum(F.when(~F.col("_delivery_on_time"), 1).otherwise(0)).alias("violations"),
            F.min("_delivery_delay_seconds").alias("min_seconds"),
            F.max("_delivery_delay_seconds").alias("max_seconds"),
            F.percentile_approx("_delivery_delay_seconds", 0.95).alias("p95_seconds"),
        )
        .first()
        .asDict()
    )
    add_check(
        "delivery_delay_sla",
        "timeliness",
        int(timing["violations"] or 0),
        details=json.dumps(timing),
    )
    processing_age = snapshot_age_check(
        datetime.fromisoformat(manifest["measured_at"]), now, max_age_seconds
    )
    add_check(
        "snapshot_processing_freshness",
        "timeliness",
        int(processing_age["passed"]),
        1,
        severity=processing_age["severity"],
        details=json.dumps(processing_age),
    )
    latest_ingestion = bronze.agg(F.max("_ingested_at")).first()[0]
    age_now = freshness_check(latest_ingestion, now, DEFAULT_MAX_AGE_SECONDS)
    add_check(
        "live_source_freshness",
        "timeliness",
        int(age_now["passed"]),
        1,
        severity="warning",
        details="historical_snapshot: " + json.dumps(age_now),
    )
    return results, timing


def build_summary(snapshot_id, counts, results, source_failures, timing, constraints):
    """Summarize mandatory failures separately from measured warnings."""
    failures = [r for r in results if not r["passed"] and r["severity"] == "error"]
    summary = {
        "snapshot_id": snapshot_id,
        "counts": {
            "bronze": counts["bronze"],
            "silver": counts["silver"],
            "quarantine": counts["quarantine"],
            "duplicates": counts["duplicates"],
            "fact": counts["fact"],
        },
        "real_data_quality": {
            "input_rows": counts["bronze"],
            "accepted_deliveries": counts["silver"] + counts["duplicates"],
            "quarantined_rows": counts["quarantine"],
            "quarantine_rate": counts["quarantine"] / counts["bronze"]
            if counts["bronze"]
            else None,
            "source_violations": source_failures,
            "delivery_timeliness": timing,
        },
        "checks": len(results),
        "failed_checks": failures,
        "warnings": [r for r in results if not r["passed"] and r["severity"] == "warning"],
        "passed": not failures,
    }
    summary["constraints"] = constraints
    return summary
