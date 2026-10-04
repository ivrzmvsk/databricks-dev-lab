# Databricks notebook source
# MAGIC %md
# MAGIC # Apply table permissions, RLS and CLS

# COMMAND ----------
import json
import sys
from pathlib import Path

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets
from lab6 import runtime
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
dbutils.widgets.text("reader_principal", "")
result = runtime.apply_governance(spark, config, dbutils.widgets.get("reader_principal"))
print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
