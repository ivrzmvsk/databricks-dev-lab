# Databricks notebook source
# MAGIC %md
# MAGIC # Build Wikimedia Gold from the versioned transformation library

# COMMAND ----------
import json
import sys
from pathlib import Path

dbutils.widgets.text('code_root', '')
dbutils.widgets.text('source_code_path', '')
root = Path(dbutils.widgets.get('code_root'))
sys.path.insert(0, dbutils.widgets.get('source_code_path') or str(root / 'lab6/src'))
sys.path.insert(0, str(root / 'lab8/src'))
from lab6.config import from_widgets
from lab8.gold import build_gold

spark.conf.set('spark.sql.session.timeZone', 'UTC')
result = build_gold(spark, from_widgets(dbutils), root)
print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
