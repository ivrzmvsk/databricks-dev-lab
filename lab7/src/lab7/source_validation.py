"""Read-only reconciliation of Lab 6 Gold with its pinned Lab 5 Silver."""
from pyspark.sql import functions as F


DIMENSION_KEYS = {
    "dim_wiki": "wiki_key", "dim_date": "date_key",
    "dim_namespace": "namespace_key", "dim_page": "page_key", "dim_editor": "editor_key",
}
AGGREGATE_TABLES = ("agg_wiki_5min", "agg_wiki_daily", "agg_page_activity")


def validate_gold_frames(fact, pinned_silver, dimensions, aggregates):
    """Validate supplied frames without credentials, table writes or notebook state."""
    n = fact.count()
    assert n > 0, "Gold fact is empty"
    assert fact.select("event_id").distinct().count() == n, "Duplicate events in fact"
    source_n = pinned_silver.select("event_id").distinct().count()
    assert source_n == n, "Gold count differs from the pinned Silver snapshot"
    for table, key in DIMENSION_KEYS.items():
        dim = dimensions[table]
        assert dim.count() == dim.select(key).distinct().count(), f"Duplicate {table} keys"
        assert dim.filter(F.col(key).isNull()).count() == 0, f"NULL {table} keys"
        assert fact.join(dim.select(key), key, "left_anti").count() == 0, f"Orphan {key}"
    net_bytes = fact.agg(F.sum("bytes_delta")).first()[0]
    for table in AGGREGATE_TABLES:
        totals = aggregates[table].agg(F.sum("edit_count"), F.sum("net_bytes_delta")).first()
        assert totals[0] == n, f"{table} count mismatch"
        assert totals[1] == net_bytes, f"{table} bytes mismatch"
    assert fact.filter(
        "bytes_delta IS NOT NULL AND bytes_delta <> bytes_added - bytes_removed"
    ).count() == 0, "Inconsistent fact byte measures"
    return source_n


def validate_source_gold(spark, config):
    """Read original tables only; use the version recorded by the Lab 6 build."""
    fact_table = config.source("fact_edits", gold=True)
    fact = spark.table(fact_table)
    version = int(spark.sql(
        f"SHOW TBLPROPERTIES {fact_table} ('lab6.source_version')"
    ).first()[1])
    silver = spark.read.option("versionAsOf", version).table(config.source("wiki_silver_lab5"))
    dimensions = {name: spark.table(config.source(name, gold=True)) for name in DIMENSION_KEYS}
    aggregates = {name: spark.table(config.source(name, gold=True)) for name in AGGREGATE_TABLES}
    source_n = validate_gold_frames(fact, silver, dimensions, aggregates)
    tables = {"fact_edits": fact, **dimensions, **aggregates}
    for name in ("ref_sitematrix_raw", "sec_user_access", "ops_volume_log"):
        tables[name] = spark.table(config.source(name, gold=True))
    return {
        "counts": {name: frame.count() for name, frame in tables.items()},
        "source_unique_events": source_n,
        "source_version": version,
        "unmatched_wikis": dimensions["dim_wiki"].filter("NOT metadata_matched").count(),
        "unmatched_namespaces": dimensions["dim_namespace"].filter("NOT metadata_matched").count(),
    }
