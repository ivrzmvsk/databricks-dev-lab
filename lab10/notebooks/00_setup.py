# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 10 — Initialize Delta data
# MAGIC Infrastructure provisioning runs separately through tools/ and the bundle.

# COMMAND ----------
import json

if "_injected_pipeline" in globals():
    pipeline = _injected_pipeline
else:
    import sys

    for name, value in {
        "code_root": "",
        "catalog": "workspace",
        "schema": "ivanrazumovskyi_lab10",
        "foreign_catalog": "ivanrazumovskyi_lab10_neon_dev",
        "gold_schema": "dev_ivanrazumovskyi_ivanrazumovskyi_lab9_gold",
    }.items():
        dbutils.widgets.text(name, value)
    sys.path.insert(0, dbutils.widgets.get("code_root") + "/src")
    from lab10.pipeline import Config, Pipeline

    config = Config(
        **{k: dbutils.widgets.get(k) for k in ("catalog", "schema", "foreign_catalog", "gold_schema")}
    )
    pipeline = Pipeline(lambda query: [r.asDict() for r in spark.sql(query).collect()], config)

if "spark" in globals():
    spark.conf.set("spark.sql.session.timeZone", "UTC")

from lab10.pipeline import FIELDS

pipeline.sql(f"CREATE SCHEMA IF NOT EXISTS {pipeline.c.catalog}.{pipeline.c.schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load the external reference into a CDF-enabled Delta source
# MAGIC This is data initialization. The UC connection and foreign catalog must already exist.

# COMMAND ----------
external = pipeline.c.foreign("wiki_projects")
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.source} USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
    AS SELECT {FIELDS} FROM {external}""")
if not pipeline.count(f"SELECT count(*) FROM {pipeline.source}"):
    raise AssertionError("Refusing to initialize from empty external projects")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preserve the initial snapshot
# MAGIC Version 0 remains the baseline on reruns; existing data is never reset.

# COMMAND ----------
# Version 0 remains the reproducible baseline even after simulations.
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.c.table("wiki_projects_snapshot")}
    USING DELTA AS SELECT {FIELDS} FROM {pipeline.source} VERSION AS OF 0""")
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.c.table("project_owners_snapshot")}
    USING DELTA AS SELECT * FROM {pipeline.c.foreign("project_owners")}""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Initialize current state, SCD2 history and the event archive

# COMMAND ----------
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.current} USING DELTA AS
    SELECT {FIELDS}, CAST(0 AS BIGINT) AS source_commit_version
    FROM {pipeline.source} VERSION AS OF 0""")
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.history} USING DELTA AS
    SELECT {FIELDS}, _commit_version AS source_commit_version,
      _commit_timestamp AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to,
      CAST(NULL AS BIGINT) AS valid_to_version, true AS is_current
    FROM table_changes('{pipeline.source}', 0, 0) WHERE _change_type='insert'""")
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.audit} USING DELTA AS
    SELECT * FROM table_changes('{pipeline.source}', 0, 0)""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Initialize the source-bound checkpoint and replay markers
# MAGIC Existing offsets are retained. A recreated source cannot reuse an old checkpoint.

# COMMAND ----------
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.checkpoint}
    (consumer STRING, source_id STRING, last_processed_version BIGINT)
    USING DELTA""")
pipeline.sql(f"""MERGE INTO {pipeline.checkpoint} t
    USING (SELECT 'wiki_projects' AS consumer, '{pipeline.detail()}' AS source_id,
                  CAST(0 AS BIGINT) AS last_processed_version) s
    ON t.consumer=s.consumer
    WHEN NOT MATCHED THEN INSERT *""")
pipeline.sql(f"""CREATE TABLE IF NOT EXISTS {pipeline.c.table("simulation_batches")}
    (batch STRING, completed_at TIMESTAMP) USING DELTA""")
state = pipeline.state()
result = {
    "baseline_projects": pipeline.count(f"SELECT count(*) FROM {pipeline.source} VERSION AS OF 0"),
    "checkpoint": state,
}
print(json.dumps(result, default=str, indent=2))
if "dbutils" in globals():
    dbutils.notebook.exit(json.dumps(result, default=str))
