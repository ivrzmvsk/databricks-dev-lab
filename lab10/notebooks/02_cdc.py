# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 10 — Apply two incremental CDC batches
# MAGIC The shared, tested CDC consumer is reused by this Job and the warehouse recovery runner.

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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Batch 1: insert, update and delete
# MAGIC Only the dedicated Delta source changes. PostgreSQL reference rows stay intact.
# MAGIC Simulation markers make reruns safe.

# COMMAND ----------
result = {"batch1": pipeline.simulate("batch1")}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Consume the first CDF range
# MAGIC The consumer freezes the upper version, applies each commit with MERGE,
# MAGIC reconciles current state and SCD2, then advances the checkpoint last.

# COMMAND ----------
result["sync1"] = pipeline.sync()
print(json.dumps(result["sync1"], default=str, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Batch 2: keep intermediate versions and a late business timestamp
# MAGIC Two enwiki updates arrive before consumption. The second has an older updated_at.
# MAGIC SCD2 follows commit order and retains this correction as a new version;
# MAGIC business-effective history would require rebuilding the affected past intervals.

# COMMAND ----------
result["batch2"] = pipeline.simulate("batch2")
result["sync2"] = pipeline.sync()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect captured change events and SCD2 versions

# COMMAND ----------
result["events"] = pipeline.sql(
    f"SELECT wiki_id, _change_type, _commit_version FROM {pipeline.audit} "
    "WHERE wiki_id IN ('enwiki', 'plwiki', 'lab10_demo') "
    "ORDER BY _commit_version, wiki_id, _change_type"
)
result["enwiki_history"] = pipeline.sql(
    f"SELECT wiki_id, owner_id, updated_at, source_commit_version, valid_to_version, is_current "
    f"FROM {pipeline.history} WHERE wiki_id='enwiki' ORDER BY source_commit_version"
)

print(json.dumps(result, default=str, indent=2))
if "dbutils" in globals():
    dbutils.notebook.exit(json.dumps(result, default=str))
