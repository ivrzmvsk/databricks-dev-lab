# Databricks notebook source
# MAGIC %md
# MAGIC # Publish constrained fact and run medallion quality gates
# MAGIC Setup is separate. Each check group is called explicitly below.
# MAGIC Reports are saved before gate failure.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters, transformations and DQX engine

# COMMAND ----------
import json
import sys
from datetime import datetime, timezone
from pyspark.sql import functions as F

dbutils.widgets.text("code_root", "")
root = dbutils.widgets.get("code_root")
for path in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, f"{root}/{path}")
from lab7.config import notebook_config

config = notebook_config(dbutils.widgets)
spark.conf.set("spark.sql.session.timeZone", "UTC")

from databricks.sdk import WorkspaceClient
from lab7.constraints import (
    NEGATIVE_WRITES,
    FACT_CONSTRAINTS,
    FACT_NOT_NULL_COLUMNS,
    constraint_error_condition,
    is_expected_constraint_error,
)
from lab7.execution import build_dqx_engine
from lab7.quality import gate
from lab7.checks import (
    record,
    check_layers,
    check_dqx,
    check_routing,
    check_dimensions,
    check_aggregates,
    check_timeliness,
    build_summary,
)
from lab7.source_validation import DIMENSION_KEYS, AGGREGATE_TABLES, validate_gold_frames
from lab7.tables import replace

dbutils.widgets.text("snapshot_max_age_seconds", "")
age_setting = dbutils.widgets.get("snapshot_max_age_seconds").strip()
snapshot_max_age_seconds = int(age_setting) if age_setting else None
dqx_engine = build_dqx_engine(spark, WorkspaceClient())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify setup and publish candidate rows with existing constraints

# COMMAND ----------
candidate = spark.table(config.table("fact_candidate"))
target = config.table("fact_edits")
actual_schema = spark.table(target).schema
assert [(f.name, f.dataType) for f in actual_schema] == [
    (f.name, f.dataType) for f in candidate.schema
], "Fact schema changed; run the setup/migration before publishing"
assert FACT_NOT_NULL_COLUMNS <= {f.name for f in actual_schema if not f.nullable}
properties = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
assert all(f"delta.constraints.{name}" in properties for name in FACT_CONSTRAINTS), (
    "Fact CHECK constraints are missing; run the separate setup Job"
)
# INSERT OVERWRITE retains the schema and constraints on each rerun.
candidate.createOrReplaceTempView("lab7_publish_fact")
spark.sql(f"INSERT OVERWRITE {target} SELECT * FROM lab7_publish_fact")
spark.catalog.dropTempView("lab7_publish_fact")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Prove all four invalid writes are rejected without changing row count

# COMMAND ----------
before = spark.table(target).count()
constraints = []
for name, expression, column in NEGATIVE_WRITES:
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
        constraints.append(
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
print(json.dumps(constraints))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load medallion tables and initialize the report

# COMMAND ----------
manifest = json.loads(spark.table(config.table("snapshot_manifest")).first().metadata_json)
run_id = manifest["snapshot_id"]
frames = {
    "bronze": spark.table(config.table("wiki_bronze")),
    "silver": spark.table(config.table("wiki_silver")),
    "quarantine": spark.table(config.table("wiki_quarantine")),
    "duplicates": spark.table(config.table("wiki_duplicates")),
    "fact": spark.table(config.table("fact_edits")),
}
counts = {name: frame.count() for name, frame in frames.items()}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Layer counts, uniqueness and row validity

# COMMAND ----------
layer_results = check_layers(frames, counts, manifest)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DQX trusted facts and source routing

# COMMAND ----------
reference_frames = {name: spark.table(config.table(name)) for name in DIMENSION_KEYS}
dqx_results, dqx_rejected, source_failures = check_dqx(frames, counts, reference_frames, dqx_engine)
replace(
    dqx_rejected.withColumn("_snapshot_id", F.lit(run_id)),
    config.table("dqx_fact_quarantine"),
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quarantine reasons and multiset delivery reconciliation

# COMMAND ----------
routing_results = check_routing(frames)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension uniqueness and referential integrity

# COMMAND ----------
dimension_results = check_dimensions(frames["fact"], reference_frames)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold aggregate reconciliation

# COMMAND ----------
aggregate_frames = {
    name: spark.table(config.table(name)) for name in ("agg_wiki_daily", "agg_wiki_5min")
}
aggregate_results = check_aggregates(frames["fact"], aggregate_frames)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Per-delivery timeliness and snapshot freshness

# COMMAND ----------
accepted = spark.table(config.table("wiki_accepted"))
now = datetime.now(timezone.utc).replace(tzinfo=None)
timeliness_results, timing = check_timeliness(
    frames["bronze"], accepted, manifest, now, snapshot_max_age_seconds
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read-only validation of original Lab 6 Gold against pinned Silver

# COMMAND ----------
source_results = []
refs = spark.table(config.source("ref_sitematrix_raw", gold=True)).select("wiki").distinct()
source_results.append(
    record(
        "wiki_reference_orphans",
        "consistency",
        frames["silver"].select("wiki").distinct().join(refs, "wiki", "left_anti").count(),
    )
)
try:
    source_fact_table = config.source("fact_edits", gold=True)
    source_fact = spark.table(source_fact_table)
    source_version = int(
        spark.sql(f"SHOW TBLPROPERTIES {source_fact_table} ('lab6.source_version')").first()[1]
    )
    pinned_silver = spark.read.option("versionAsOf", source_version).table(
        config.source("wiki_silver_lab5")
    )
    source_dimensions = {
        name: spark.table(config.source(name, gold=True)) for name in DIMENSION_KEYS
    }
    source_aggregates = {
        name: spark.table(config.source(name, gold=True)) for name in AGGREGATE_TABLES
    }
    source_count = validate_gold_frames(
        source_fact, pinned_silver, source_dimensions, source_aggregates
    )
    source_tables = {"fact_edits": source_fact, **source_dimensions, **source_aggregates}
    for name in ("ref_sitematrix_raw", "sec_user_access", "ops_volume_log"):
        source_tables[name] = spark.table(config.source(name, gold=True))
    existing = {
        "counts": {name: frame.count() for name, frame in source_tables.items()},
        "source_unique_events": source_count,
        "source_version": source_version,
        "unmatched_wikis": source_dimensions["dim_wiki"].filter("NOT metadata_matched").count(),
        "unmatched_namespaces": source_dimensions["dim_namespace"]
        .filter("NOT metadata_matched")
        .count(),
    }
except AssertionError as exc:
    source_results.append(
        record("lab6_pinned_reconciliation", "consistency", 0, 1, details=str(exc))
    )
else:
    source_results.append(
        record("lab6_pinned_reconciliation", "consistency", 1, 1, details=json.dumps(existing))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Persist the complete DQ report before enforcing the gate

# COMMAND ----------
results = [
    *layer_results,
    *dqx_results,
    *routing_results,
    *dimension_results,
    *aggregate_results,
    *timeliness_results,
    *source_results,
]
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build the summary and enforce mandatory checks

# COMMAND ----------
summary = build_summary(run_id, counts, results, source_failures, timing, constraints)
print(json.dumps(summary, default=str))
gate(results)
dbutils.notebook.exit(json.dumps(summary, default=str))
