# Databricks notebook source
# MAGIC %md
# MAGIC # Freeze real Wikipedia deliveries and inspect demo quarantine
# MAGIC Requires the separate setup Job. Lab 5 source tables are read-only.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters and transformations

# COMMAND ----------
import json
import sys
from datetime import datetime, timezone
from pyspark.sql import functions as F

dbutils.widgets.text("code_root", "")
root = dbutils.widgets.get("code_root")
for path in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, f"{root}/{path}")
from lab7.config import notebook_config

config = notebook_config(dbutils.widgets)
spark.conf.set("spark.sql.session.timeZone", "UTC")

import uuid
from lab7.constraints import BRONZE_SCHEMA
from lab7.fixtures import demo_rows
from lab7.quality import classify
from lab7.tables import replace

# Fail early if setup was not run; never provision infrastructure in this task.
spark.table(config.table("fact_edits")).limit(0).collect()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pin the source Delta version and copy real rows only

# COMMAND ----------
spark.conf.set("spark.sql.session.timeZone", "UTC")
source = config.source("wiki_bronze_lab5")
version = spark.sql(f"DESCRIBE HISTORY {source}").agg(F.max("version")).first()[0]
run_id = str(uuid.uuid4())
bronze = spark.read.option("versionAsOf", version).table(source)
real_count = bronze.count()
if not real_count:
    raise AssertionError("Lab 5 Bronze is empty")
real = bronze.withColumn("_snapshot_id", F.lit(run_id))
replace(real, config.table("bronze_snapshot"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save snapshot provenance

# COMMAND ----------
measured_at = datetime.now(timezone.utc).replace(tzinfo=None)
metadata = {
    "snapshot_id": run_id,
    "source": source,
    "version": int(version),
    "real_rows": real_count,
    "measured_at": measured_at.isoformat(),
    "freshness_mode": "historical_snapshot",
}
replace(
    spark.createDataFrame(
        [(run_id, json.dumps(metadata))], "snapshot_id STRING, metadata_json STRING"
    ),
    config.table("snapshot_manifest"),
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Persist and inspect synthetic rejects in a separate demo table

# COMMAND ----------
source = spark.createDataFrame(demo_rows(), BRONZE_SCHEMA)
rejected = (
    classify(source).filter("NOT _is_valid").withColumn("demo_run_id", F.lit(str(uuid.uuid4())))
)
target = config.table("wiki_quarantine_demo")
replace(rejected, target)
saved = spark.table(target)
count = saved.count()
if count != 6 or saved.filter("_is_valid OR size(_quality_errors) = 0").count():
    raise AssertionError(
        "Persisted quarantine demo did not retain six rejected fixtures with reasons"
    )
metadata["quarantine_demo"] = {
    "table": target,
    "input_rows": len(demo_rows()),
    "rejected_rows": count,
    "reasons": [
        row.asDict()
        for row in saved.select("event_id", "kafka_offset", "_quality_errors")
        .orderBy("kafka_offset")
        .collect()
    ],
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Snapshot output

# COMMAND ----------
print(json.dumps(metadata))
dbutils.notebook.exit(json.dumps(metadata))
