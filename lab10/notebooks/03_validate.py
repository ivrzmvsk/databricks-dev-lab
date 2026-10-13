# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 10 — Validate CDC results and a no-op rerun
# MAGIC Task-specific acceptance checks are visible here; shared reconciliation remains in the CDC library.

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
# MAGIC ## Verify inserts, updates, deletes and intermediate SCD2 history

# COMMAND ----------
result = {}
if pipeline.count(f"SELECT count(*) FROM {pipeline.current} WHERE wiki_id='plwiki'"):
    raise AssertionError("Deleted project remains in target")
if pipeline.count(f"SELECT count(*) FROM {pipeline.current} WHERE wiki_id='lab10_demo'") != 1:
    raise AssertionError("Inserted demo project missing or duplicated")
if pipeline.count(f"SELECT count(*) FROM {pipeline.current} WHERE wiki_id='enwiki' AND owner_id=3") != 1:
    raise AssertionError("Late committed update was not applied")
if pipeline.count(f"SELECT count(*) FROM {pipeline.history} WHERE wiki_id='enwiki'") != 4:
    raise AssertionError("Intermediate SCD2 updates were lost")
pipeline.equal(
    f"SELECT * FROM {pipeline.changes(0, pipeline.state())}",
    f"SELECT * FROM {pipeline.audit}",
    "Full demonstration audit mismatch",
)
result["validation"] = pipeline.validate()
# COMMAND ----------
# MAGIC %md
# MAGIC ## Rerun without new changes
# MAGIC The checkpoint and target counts must stay unchanged. Full row reconciliation
# MAGIC and event-key checks run inside validate(), not just count comparisons.

# COMMAND ----------
before = pipeline.validate()
result["rerun"] = pipeline.sync()
assert result["rerun"]["status"] == "NO_NEW_CHANGES"
assert pipeline.validate() == before, "No-op rerun changed the targets"

print(json.dumps(result, default=str, indent=2))
if "dbutils" in globals():
    dbutils.notebook.exit(json.dumps(result, default=str))
