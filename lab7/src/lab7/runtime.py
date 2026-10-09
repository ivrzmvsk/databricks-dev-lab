"""Side effects live here; pure transformation and DQ functions live in quality.py."""

import json
import uuid
from datetime import datetime, timezone
from functools import partial

from pyspark.sql import functions as F

from lab7.quality import (
    DIMENSIONS,
    ROW_RULES,
    FACT_RULES,
    classify,
    evaluate_rows,
    reconcile_deliveries,
    freshness_check,
    delivery_timeliness,
    snapshot_age_check,
    gate,
)
from lab7.source_validation import validate_source_gold


def replace(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def snapshot(spark, config):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.namespace}")
    source = config.source("wiki_bronze_lab5")
    version = spark.sql(f"DESCRIBE HISTORY {source}").agg(F.max("version")).first()[0]
    run_id = str(uuid.uuid4())
    bronze = spark.read.option("versionAsOf", version).table(source)
    real_count = bronze.count()
    if not real_count:
        raise AssertionError("Lab 5 Bronze is empty")
    real = bronze.withColumn("_snapshot_id", F.lit(run_id))
    replace(real, config.table("bronze_snapshot"))
    measured_at = datetime.now(timezone.utc).replace(tzinfo=None)
    metadata = {
        "snapshot_id": run_id,
        "source": source,
        "version": int(version),
        "real_rows": real_count,
        "measured_at": measured_at.isoformat(),
        "freshness_mode": "historical_snapshot",
    }
    replace(
        spark.createDataFrame(
            [(run_id, json.dumps(metadata))], "snapshot_id STRING, metadata_json STRING"
        ),
        config.table("snapshot_manifest"),
    )
    metadata["quarantine_demo"] = persist_quarantine_demo(spark, config)
    return metadata


def persist_quarantine_demo(spark, config):
    """Persist rejected synthetic examples in a separate table, never in the medallion."""
    from lab7.fixtures import demo_rows

    schema = (
        "event_json STRING, kafka_topic STRING, kafka_partition INT, "
        "kafka_offset BIGINT, kafka_enqueued_at TIMESTAMP, _ingested_at "
        "TIMESTAMP"
    )
    source = spark.createDataFrame(demo_rows(), schema)
    rejected = (
        classify(source).filter("NOT _is_valid").withColumn("demo_run_id", F.lit(str(uuid.uuid4())))
    )
    target = config.table("wiki_quarantine_demo")
    replace(rejected, target)
    saved = spark.table(target)
    count = saved.count()
    if count != 6 or saved.filter("_is_valid OR size(_quality_errors) = 0").count():
        raise AssertionError(
            "Persisted quarantine demo did not retain six rejected fixtures with reasons"
        )
    return {
        "table": target,
        "input_rows": len(demo_rows()),
        "rejected_rows": count,
        "reasons": [
            row.asDict()
            for row in saved.select("event_id", "kafka_offset", "_quality_errors")
            .orderBy("kafka_offset")
            .collect()
        ],
    }


def publish_constrained_fact(spark, config):
    """Publish a regular Delta table, independent of pipeline-owned MV lifecycle."""
    candidate = spark.table(config.table("fact_candidate"))
    target = config.table("fact_edits")
    schema = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + (
            " NOT NULL"
            if field.name
            in {
                "event_id",
                "wiki_key",
                "date_key",
                "page_key",
                "editor_key",
                "namespace_key",
                "edit_count",
            }
            else ""
        )
        for field in candidate.schema.fields
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({schema}) USING DELTA")
    constraints = {
        "one_edit": "edit_count = 1",
        "lengths_nonnegative": (
            "(old_length IS NULL OR old_length >= 0) AND (new_length IS NULL OR new_length >= 0)"
        ),
        "bytes_consistent": FACT_RULES["bytes_consistent"],
    }
    props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    for name, expression in constraints.items():
        existing = props.get(f"delta.constraints.{name}")
        if existing is None:
            spark.sql(f"ALTER TABLE {target} ADD CONSTRAINT {name} CHECK ({expression})")
    # INSERT OVERWRITE retains the schema and constraints on each rerun.
    candidate.createOrReplaceTempView("lab7_publish_fact")
    spark.sql(f"INSERT OVERWRITE {target} SELECT * FROM lab7_publish_fact")
    spark.catalog.dropTempView("lab7_publish_fact")
    before = spark.table(target).count()
    attempts = []
    for name, expression, column in [
        ("not_null_rejected", "CAST(NULL AS STRING)", "event_id"),
        ("check_rejected", "CAST(-1 AS BIGINT)", "edit_count"),
        ("lengths_nonnegative_rejected", "CAST(-1 AS BIGINT)", "old_length"),
        (
            "bytes_consistent_rejected",
            "CAST(coalesce(bytes_added, 0) - coalesce(bytes_removed, 0) + 1 AS BIGINT)",
            "bytes_delta",
        ),
    ]:
        projection = ", ".join(
            f"{expression} AS `{c}`" if c == column else f"`{c}`" for c in candidate.columns
        )
        try:
            spark.sql(
                f"INSERT INTO {target} SELECT {projection} "
                f"FROM {config.table('fact_candidate')} LIMIT 1"
            )
        except Exception as exc:
            error_class = constraint_error_condition(exc)
            parameters = exc.getMessageParameters() if hasattr(exc, "getMessageParameters") else {}
            if not is_expected_constraint_error(error_class, column, parameters):
                raise
            attempts.append(
                {
                    "check": name,
                    "passed": True,
                    "error_class": error_class,
                    "message_parameters": parameters,
                }
            )
        else:
            raise AssertionError(f"Constraint did not reject {name}")
    if spark.table(target).count() != before:
        raise AssertionError("Rejected writes changed the fact table")
    return attempts


def constraint_error_condition(exc):
    for accessor in ("getCondition", "getErrorClass"):
        method = getattr(exc, accessor, None)
        if method:
            condition = method()
            if condition:
                return condition
    return None


def is_expected_constraint_error(condition, column, parameters=None):
    if column == "event_id":
        return condition == "DELTA_NOT_NULL_CONSTRAINT_VIOLATED"
    constraint = {
        "edit_count": "one_edit",
        "old_length": "lengths_nonnegative",
        "bytes_delta": "bytes_consistent",
    }.get(column)
    if constraint:
        return (parameters or {}).get("constraintName") == constraint and condition in {
            "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
            "DELTA_VIOLATE_CONSTRAINT_WITHOUT_VALUES",
            "DELTA_CHECK_CONSTRAINT_VIOLATED",
        }
    return False


def _record_result(results, name, dimension, observed, expected=0, severity="error", details=""):
    results.append(
        {
            "check": name,
            "dimension": dimension,
            "observed": float(observed),
            "expected": float(expected),
            "passed": observed == expected,
            "severity": severity,
            "details": str(details),
        }
    )


def _check_layers(frames, counts, manifest, record):
    silver, fact = frames["silver"], frames["fact"]
    bronze_count, silver_count, quarantine_count, duplicate_count, fact_count = (
        counts[name] for name in ("bronze", "silver", "quarantine", "duplicates", "fact")
    )
    record("bronze_rows_present", "completeness", int(bronze_count > 0), 1)
    record(
        "branch_reconciliation",
        "consistency",
        silver_count + quarantine_count + duplicate_count,
        bronze_count,
    )
    record("snapshot_reconciliation", "consistency", bronze_count, manifest["real_rows"])
    record("silver_to_fact", "consistency", fact_count, silver_count)
    record(
        "silver_key_uniqueness",
        "uniqueness",
        silver_count - silver.select("event_id").distinct().count(),
    )
    record(
        "fact_key_uniqueness", "uniqueness", fact_count - fact.select("event_id").distinct().count()
    )
    for name, failures in evaluate_rows(silver, ROW_RULES).items():
        record("silver_" + name, DIMENSIONS[name], failures)
    for name, failures in evaluate_rows(fact, FACT_RULES).items():
        record("fact_" + name, "consistency", failures)


def _check_dqx(spark, config, dqx_engine, frames, counts, run_id, record):
    from lab7.dqx_checks import (
        apply_trusted,
        failures_by_rule,
        predicate_checks,
        trusted_checks,
    )

    bronze, fact = frames["bronze"], frames["fact"]
    quarantine_count = counts["quarantine"]
    reference_keys = {
        "dim_wiki": "wiki_key",
        "dim_page": "page_key",
        "dim_editor": "editor_key",
        "dim_namespace": "namespace_key",
        "dim_date": "date_key",
    }
    reference_frames = {table: spark.table(config.table(table)) for table in reference_keys}
    trusted = apply_trusted(fact, FACT_RULES, reference_keys, reference_frames, dqx_engine)
    failed_dqx = failures_by_rule(trusted)
    for check in trusted_checks(FACT_RULES, reference_keys):
        name = check["name"]
        dimension = "uniqueness" if name == "event_id_unique" else "consistency"
        if name == "required_fact_keys":
            dimension = "completeness"
        record("dqx_fact_" + name, dimension, failed_dqx.get(name, 0))
    rejected = trusted.filter("_errors IS NOT NULL").withColumn("_snapshot_id", F.lit(run_id))
    replace(rejected, config.table("dqx_fact_quarantine"))
    record("dqx_fact_rejected_rows", "validity", rejected.count())
    source_checked = dqx_engine.apply_checks_by_metadata(
        classify(bronze).drop("_quality_errors", "_is_valid"),
        predicate_checks(ROW_RULES),
    )
    source_failures = failures_by_rule(source_checked)
    record(
        "dqx_source_routing",
        "consistency",
        source_checked.filter("_errors IS NOT NULL").count(),
        quarantine_count,
    )
    return source_failures


def _check_routing(frames, record):
    bronze, silver, quarantine, duplicates = (
        frames[name] for name in ("bronze", "silver", "quarantine", "duplicates")
    )
    record(
        "quarantine_reasons",
        "completeness",
        quarantine.filter("size(_quality_errors) = 0 OR _quality_errors IS NULL").count(),
    )
    record("quarantine_valid_leak", "consistency", quarantine.filter("_is_valid").count())
    # Branch counts alone could conceal one missing row and one extra row.
    delivery_key = ["kafka_topic", "kafka_partition", "kafka_offset"]
    for name, count in reconcile_deliveries(
        bronze, [silver, quarantine, duplicates], delivery_key
    ).items():
        record("routing_" + name, "consistency", count)


def _check_dimensions(spark, config, fact, record):
    for table, key in [
        ("dim_wiki", "wiki_key"),
        ("dim_page", "page_key"),
        ("dim_editor", "editor_key"),
        ("dim_namespace", "namespace_key"),
        ("dim_date", "date_key"),
    ]:
        dimension = spark.table(config.table(table))
        record(
            table + "_uniqueness",
            "uniqueness",
            dimension.count() - dimension.select(key).distinct().count(),
        )
        record(table + "_null_keys", "completeness", dimension.filter(F.col(key).isNull()).count())
        record(
            table + "_orphans",
            "consistency",
            fact.join(dimension.select(key), key, "left_anti").count(),
        )


def _check_aggregates(spark, config, fact, record):
    metrics = ["edit_count", "bytes_added", "bytes_removed"]
    baseline = fact.agg(
        *[F.sum(c).alias(c) for c in metrics], F.sum("bytes_delta").alias("net_bytes_delta")
    ).first()
    for table in ["agg_wiki_daily", "agg_wiki_5min"]:
        aggregate = spark.table(config.table(table))
        totals = aggregate.agg(*[F.sum(c).alias(c) for c in metrics + ["net_bytes_delta"]]).first()
        for measure in metrics + ["net_bytes_delta"]:
            # NULL totals and zero totals have different meanings.
            record(
                table + "_" + measure,
                "consistency",
                int(totals[measure] == baseline[measure]),
                1,
                details=f"aggregate={totals[measure]}, fact={baseline[measure]}",
            )


def _check_timeliness(spark, config, bronze, manifest, record, snapshot_max_age_seconds):
    # Check every accepted delivery; independent maxima can hide stale outliers.
    timed = delivery_timeliness(spark.table(config.table("wiki_accepted")))
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
    record(
        "delivery_delay_sla",
        "timeliness",
        int(timing["violations"] or 0),
        details=json.dumps(timing),
    )
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    processing_age = snapshot_age_check(
        datetime.fromisoformat(manifest["measured_at"]), now, snapshot_max_age_seconds
    )
    record(
        "snapshot_processing_freshness",
        "timeliness",
        int(processing_age["passed"]),
        1,
        severity=processing_age["severity"],
        details=json.dumps(processing_age),
    )
    latest_ingestion = bronze.agg(F.max("_ingested_at")).first()[0]
    age_now = freshness_check(latest_ingestion, now, 3600)
    record(
        "live_source_freshness",
        "timeliness",
        int(age_now["passed"]),
        1,
        severity="warning",
        details="historical_snapshot: " + json.dumps(age_now),
    )
    return timing


def _check_source_tables(spark, config, silver, record):
    refs = spark.table(config.source("ref_sitematrix_raw", gold=True)).select("wiki").distinct()
    record(
        "wiki_reference_orphans",
        "consistency",
        silver.select("wiki").distinct().join(refs, "wiki", "left_anti").count(),
    )
    # Validate the original Lab 6 medallion at its pinned Lab 5 Silver version.
    try:
        existing = validate_source_gold(spark, config)
    except AssertionError as exc:
        record("lab6_pinned_reconciliation", "consistency", 0, 1, details=str(exc))
    else:
        record("lab6_pinned_reconciliation", "consistency", 1, 1, details=json.dumps(existing))


def _persist_results(spark, config, run_id, results):
    # Persist reports before raising so failed gates remain inspectable.
    report = spark.createDataFrame(
        [
            (
                run_id,
                r["check"],
                r["dimension"],
                r["severity"],
                r["observed"],
                r["expected"],
                r["passed"],
                r["details"],
            )
            for r in results
        ],
        (
            "snapshot_id STRING, check_name STRING, dimension STRING, severity "
            "STRING, observed DOUBLE, expected DOUBLE, passed BOOLEAN, details "
            "STRING"
        ),
    ).withColumn("checked_at", F.current_timestamp())
    report.write.format("delta").mode("append").saveAsTable(config.table("dq_results"))


def _suite_summary(run_id, counts, results, source_failures, timing):
    bronze_count, silver_count, quarantine_count, duplicate_count, fact_count = (
        counts[name] for name in ("bronze", "silver", "quarantine", "duplicates", "fact")
    )
    failures = [r for r in results if not r["passed"] and r["severity"] == "error"]
    summary = {
        "snapshot_id": run_id,
        "counts": {
            "bronze": bronze_count,
            "silver": silver_count,
            "quarantine": quarantine_count,
            "duplicates": duplicate_count,
            "fact": fact_count,
        },
        "real_data_quality": {
            "input_rows": bronze_count,
            "accepted_deliveries": silver_count + duplicate_count,
            "quarantined_rows": quarantine_count,
            "quarantine_rate": quarantine_count / bronze_count if bronze_count else None,
            "source_violations": source_failures,
            "delivery_timeliness": timing,
        },
        "checks": len(results),
        "failed_checks": failures,
        "warnings": [r for r in results if not r["passed"] and r["severity"] == "warning"],
        "passed": not failures,
    }
    return summary


def run_suite(spark, config, dqx_engine, *, snapshot_max_age_seconds=None):
    """Run medallion checks, persist their report, then enforce the quality gate."""
    manifest = json.loads(spark.table(config.table("snapshot_manifest")).first().metadata_json)
    run_id = manifest["snapshot_id"]
    results = []
    record = partial(_record_result, results)
    frames = {
        "bronze": spark.table(config.table("wiki_bronze")),
        "silver": spark.table(config.table("wiki_silver")),
        "quarantine": spark.table(config.table("wiki_quarantine")),
        "duplicates": spark.table(config.table("wiki_duplicates")),
        "fact": spark.table(config.table("fact_edits")),
    }
    counts = {name: frame.count() for name, frame in frames.items()}

    _check_layers(frames, counts, manifest, record)
    source_failures = _check_dqx(spark, config, dqx_engine, frames, counts, run_id, record)
    _check_routing(frames, record)
    _check_dimensions(spark, config, frames["fact"], record)
    _check_aggregates(spark, config, frames["fact"], record)
    timing = _check_timeliness(
        spark, config, frames["bronze"], manifest, record, snapshot_max_age_seconds
    )
    _check_source_tables(spark, config, frames["silver"], record)

    _persist_results(spark, config, run_id, results)
    summary = _suite_summary(run_id, counts, results, source_failures, timing)
    gate(results)
    return summary
