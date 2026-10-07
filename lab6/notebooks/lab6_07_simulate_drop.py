# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate a drop; the next task records its volume

# COMMAND ----------
import json
import sys
from pathlib import Path

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets
from lab6.security import assert_policies
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify policies and baseline, then simulate the drop

# COMMAND ----------
assert_policies(spark, config)
assert spark.table(config.table('ops_volume_log')).filter("scenario = 'normal' AND NOT should_alert").count() > 0, 'Record a healthy baseline first'
before = spark.table(config.table('fact_edits')).count()
# Delete only the Lab 6 fact. The demo Job must always rebuild it afterwards.
spark.sql(f"DELETE FROM {config.table('fact_edits')} WHERE pmod(xxhash64(event_id), 10) <> 0")
after = spark.table(config.table('fact_edits')).count()
result = {'before': before, 'after': after}

print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
