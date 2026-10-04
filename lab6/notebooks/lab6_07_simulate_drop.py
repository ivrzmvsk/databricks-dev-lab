# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate a volume drop in Lab 6 only

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
result = runtime.simulate_drop(spark, config)
print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
