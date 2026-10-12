# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 9: submit a notebook on managed serverless compute
# MAGIC A bounded Spark computation proves execution. No persistent cluster is created.

# COMMAND ----------
# ruff: noqa: F821
import json

total = spark.range(10).groupBy().sum("id").first()[0]
assert total == 45, "Unexpected Spark result"
dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "sum": total}))
