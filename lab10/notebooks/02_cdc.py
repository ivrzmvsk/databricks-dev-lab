# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 10 — Two CDC batches, SCD2 history, retry and no-op rerun
# MAGIC SCD2 uses commit order. A late business timestamp is retained as an attribute;
# MAGIC it does not retroactively rewrite effective-time intervals.

# COMMAND ----------
import json
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

spark.conf.set("spark.sql.session.timeZone", "UTC")
config = Config(
    **{k: dbutils.widgets.get(k) for k in ("catalog", "schema", "foreign_catalog", "gold_schema")}
)
pipeline = Pipeline(lambda query: [r.asDict() for r in spark.sql(query).collect()], config)
result = {"batch1": pipeline.simulate("batch1"), "sync1": pipeline.sync()}
result["batch2"] = pipeline.simulate("batch2")
result["sync2"] = pipeline.sync()
before = pipeline.validate()
result["rerun"] = pipeline.sync()
assert result["rerun"]["status"] == "NO_NEW_CHANGES"
assert pipeline.validate() == before
result["validation"] = pipeline.assert_scenario()
print(json.dumps(result, default=str, indent=2))
dbutils.notebook.exit(json.dumps(result, default=str))
