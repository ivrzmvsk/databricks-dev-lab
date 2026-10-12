# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 10 — Query Neon, compare Delta, enrich Wikipedia facts
# MAGIC Timings include compute and client overhead; small samples and caches limit conclusions.

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

config = Config(
    **{k: dbutils.widgets.get(k) for k in ("catalog", "schema", "foreign_catalog", "gold_schema")}
)
pipeline = Pipeline(lambda query: [r.asDict() for r in spark.sql(query).collect()], config)
result = pipeline.federation()
print(json.dumps(result, default=str, indent=2))
dbutils.notebook.exit(json.dumps(result, default=str))
