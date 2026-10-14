# Databricks notebook source
# ruff: noqa: F821, E402
# MAGIC %pip install databricks-sdk==0.152.0 tiktoken==0.14.0

# COMMAND ----------
import json
import sys
from pathlib import Path

root = Path(dbutils.widgets.get("code_root"))
sys.path.insert(0, str(root / "src"))

from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from lab12.notebook import initialize
from lab12.release import build_hash
from lab12.storage import merge_snapshot, snapshot_rows

context = initialize(root, dbutils)
settings = context.settings
initial_build = context.build
dbutils.jobs.taskValues.set(key="build_hash", value=initial_build)

# COMMAND ----------
# Load, clean and split the frozen catalogue into bounded chunks.
documents, chunks = context.documents, context.chunks

# COMMAND ----------
# Update existing tables. Infrastructure is provisioned by tools/setup_resources.py.
results = {}
for table, rows, key in snapshot_rows(documents, chunks):
    # Explicit schema avoids Spark inference problems with empty/missing fields.
    types = {bool: BooleanType(), int: LongType(), str: StringType()}
    fields = [StructField(k, types[type(v)], False) for k, v in rows[0].items()]
    frame = spark.createDataFrame(rows, schema=StructType(fields))
    results[table] = merge_snapshot(
        spark, settings.table(table), frame, rows, key, f"lab12_{table}_incoming"
    )
if build_hash(root) != initial_build:
    raise ValueError("Source changed during ingestion; rerun the frozen build")
dbutils.notebook.exit(json.dumps(results))
