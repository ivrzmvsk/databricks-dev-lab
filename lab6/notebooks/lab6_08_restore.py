# Databricks notebook source
# MAGIC %md
# MAGIC # Restore Gold and record recovery

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
runtime.build_gold(spark, config)
runtime.apply_governance(spark, config)
runtime.validate_gold(spark, config)
result = runtime.record_volume(spark, config, "recovered")
print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
