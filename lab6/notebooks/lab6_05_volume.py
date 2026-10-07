# Databricks notebook source
# MAGIC %md
# MAGIC # Record normal, simulated-drop or recovered snapshot volume

# COMMAND ----------
import json
import sys
import uuid
from pathlib import Path
from pyspark.sql import functions as F

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Select the measurement scenario

# COMMAND ----------
dbutils.widgets.text('scenario', 'normal')
scenario = dbutils.widgets.get('scenario')
if scenario not in {'normal', 'simulated_drop', 'recovered'}:
    raise ValueError('Unknown volume scenario')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Measure the current fact and pinned source

# COMMAND ----------
fact = spark.table(config.table('fact_edits'))
observed = fact.count()
version = spark.sql(f"SHOW TBLPROPERTIES {config.table('fact_edits')} ('lab6.source_version')").first()[1]
source_count = spark.read.option('versionAsOf', int(version)).table(config.source).select('event_id').distinct().count()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Preserve the last healthy baseline

# COMMAND ----------
prior = spark.table(config.table('ops_volume_log')).filter(
    "scenario IN ('normal','recovered') AND NOT should_alert")\
    .orderBy(F.col('measured_at').desc()).first()
if scenario != 'normal' and prior is None:
    raise ValueError('Record a healthy baseline before running the volume demo')
# A rebuild after source data loss must not silently lower the healthy baseline.
expected = max(source_count, prior.expected_count if prior else 0)
drop = max(0.0, 100.0 * (expected - observed) / expected) if expected else 0.0

# COMMAND ----------
# MAGIC %md
# MAGIC ## Append a measurement to the operational log

# COMMAND ----------
coverage = fact.agg(F.min('event_time'), F.max('event_time'), F.countDistinct('bucket_start')).first()
row = (str(uuid.uuid4()), scenario, source_count, observed, expected, drop,
       config.drop_threshold_pct, bool(expected and drop >= config.drop_threshold_pct),
       coverage[0], coverage[1], coverage[2])
schema = ('run_id STRING, scenario STRING, source_count LONG, observed_count LONG, '
          'expected_count LONG, drop_pct DOUBLE, threshold_pct DOUBLE, should_alert BOOLEAN, '
          'first_event TIMESTAMP, last_event TIMESTAMP, observed_5min_buckets LONG')
spark.createDataFrame([row], schema).withColumn('measured_at', F.current_timestamp())\
    .select(*spark.table(config.table('ops_volume_log')).columns)\
    .write.format('delta').mode('append').saveAsTable(config.table('ops_volume_log'))
result = {'scenario': scenario, 'observed': observed, 'expected': expected, 'drop_pct': drop,
            'should_alert': row[7]}

print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
