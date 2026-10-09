"""Shared predicates for expectations, pytest, and the headless DQ gate."""

from datetime import datetime

from pyspark.sql import Window, functions as F

from lab5.contracts import WIKI_RULES
from lab5.transforms import annotate_quality, parse_wiki
from lab6.transforms import prepare_edits, aggregate_activity


DEFAULT_MAX_AGE_SECONDS = 3600


ROW_RULES = {
    **WIKI_RULES,
    "required_namespace": "namespace IS NOT NULL",
    "wiki_format": "wiki RLIKE '^[a-zA-Z0-9_-]+$'",
}
DIMENSIONS = {
    "valid_json": "validity",
    "valid_event_id": "completeness",
    "edit_event": "validity",
    "valid_title": "completeness",
    "valid_user": "completeness",
    "valid_wiki": "completeness",
    "valid_domain": "validity",
    "valid_timestamp": "validity",
    "valid_lengths": "validity",
    "required_namespace": "completeness",
    "wiki_format": "validity",
}
FACT_RULES = {
    "required_fact_keys": (
        "event_id IS NOT NULL AND wiki_key IS NOT NULL AND date_key IS NOT NULL"
        " AND page_key IS NOT NULL AND editor_key IS NOT NULL AND namespace_key"
        " IS NOT NULL"
    ),
    "one_edit": "edit_count = 1",
    "bytes_consistent": (
        "bytes_delta IS NULL OR (bytes_added IS NOT NULL AND bytes_removed IS "
        "NOT NULL AND bytes_delta = bytes_added - bytes_removed AND bytes_added"
        " >= 0 AND bytes_removed >= 0)"
    ),
}


def classify(bronze):
    """Pure source transformation: no DQX engine, credentials or environment."""
    return annotate_quality(parse_wiki(bronze), ROW_RULES)


def rank_deliveries(accepted):
    order = Window.partitionBy("event_id").orderBy(
        "kafka_enqueued_at", "kafka_topic", "kafka_partition", "kafka_offset"
    )
    return accepted.withColumn("_delivery", F.row_number().over(order))


def make_fact(silver):
    return prepare_edits(silver)


def make_dimensions(fact):
    return {
        "dim_wiki": fact.select("wiki_key", "wiki").distinct(),
        "dim_page": fact.select("page_key", "wiki", "title").distinct(),
        "dim_editor": fact.select("editor_key", "wiki", "user_name").distinct(),
        "dim_namespace": fact.select("namespace_key", "wiki", "namespace").distinct(),
        "dim_date": fact.select("date_key", "event_date").distinct(),
    }


def enrich_wikis(wikis, reference):
    """Keep every observed wiki, attaching Lab 6 SiteMatrix metadata by wiki ID."""
    return wikis.join(reference.select("wiki", "site_url"), "wiki", "left").withColumn(
        "metadata_matched", F.col("site_url").isNotNull()
    )


def make_aggregates(fact):
    return {
        "agg_wiki_daily": aggregate_activity(fact, ["wiki", "wiki_key", "date_key", "event_date"]),
        "agg_wiki_5min": aggregate_activity(fact, ["wiki", "wiki_key", "date_key", "bucket_start"]),
    }


def evaluate_rows(df, rules):
    """One Spark scan per dataset, counting SQL NULL as a failed predicate."""
    counts = (
        df.agg(
            F.count("*").alias("_count"),
            *[
                F.sum(F.when(~F.coalesce(F.expr(expr), F.lit(False)), 1).otherwise(0)).alias(name)
                for name, expr in rules.items()
            ],
        )
        .first()
        .asDict()
    )
    return {name: int(counts[name] or 0) for name in rules}


def reconcile_deliveries(bronze, branches, keys):
    """Compare multisets, so equal totals cannot conceal missing/extra rows."""
    routed = branches[0].select(*keys)
    for branch in branches[1:]:
        routed = routed.unionByName(branch.select(*keys))
    original = bronze.select(*keys)
    return {
        "missing_rows": original.exceptAll(routed).count(),
        "extra_rows": routed.exceptAll(original).count(),
    }


def freshness_check(latest, reference: datetime, max_age_seconds=DEFAULT_MAX_AGE_SECONDS):
    """Explicit reference time makes snapshot and live freshness testable."""
    if latest is None or reference is None:
        return {"passed": False, "age_seconds": None, "reason": "empty_timestamp"}
    age = (reference - latest).total_seconds()
    return {
        "passed": 0 <= age <= max_age_seconds,
        "age_seconds": age,
        "reason": "fresh" if 0 <= age <= max_age_seconds else "stale_or_future",
    }


def snapshot_age_check(captured_at, reference, max_age_seconds=None):
    """Manual runs measure snapshot age; a caller-supplied SLA makes it mandatory."""
    if max_age_seconds is not None and max_age_seconds <= 0:
        raise ValueError("Snapshot age SLA must be a positive number of seconds")
    result = freshness_check(captured_at, reference, max_age_seconds or DEFAULT_MAX_AGE_SECONDS)
    result["severity"] = "warning" if max_age_seconds is None else "error"
    result["mode"] = "measure_only" if max_age_seconds is None else "enforced_sla"
    return result


def delivery_timeliness(df, max_delay_seconds=DEFAULT_MAX_AGE_SECONDS):
    """Check each event against its own ingestion timestamp, including outliers."""
    delay = F.col("_ingested_at").cast("double") - F.col("event_time").cast("double")
    return df.withColumn("_delivery_delay_seconds", delay).withColumn(
        "_delivery_on_time", F.coalesce(delay.between(0, max_delay_seconds), F.lit(False))
    )


def gate(results):
    failed = [r["check"] for r in results if r["severity"] == "error" and not r["passed"]]
    if failed:
        raise AssertionError("Data quality gate failed: " + ", ".join(failed))
