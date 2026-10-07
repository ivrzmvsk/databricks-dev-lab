# Databricks notebook source
# MAGIC %md
# MAGIC # Provision Lab 6 schema, Volume and operational tables

# COMMAND ----------
import json
import sys
from pathlib import Path

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets
from lab6.tables import sql_string
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the isolated schema and metadata Volume

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.namespace}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.namespace}.lab6_assets")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create access mappings and volume history

# COMMAND ----------
spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.table('sec_user_access')} (
    username STRING NOT NULL, wiki STRING NOT NULL, can_view_editor BOOLEAN NOT NULL,
    access_reason STRING, updated_at TIMESTAMP
) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.table('ops_volume_log')} (
    run_id STRING, measured_at TIMESTAMP, scenario STRING, source_count BIGINT,
    observed_count BIGINT, expected_count BIGINT, drop_pct DOUBLE,
    threshold_pct DOUBLE, should_alert BOOLEAN, first_event TIMESTAMP,
    last_event TIMESTAMP, observed_5min_buckets BIGINT
) USING DELTA""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Provision owner access without overwriting existing mappings

# COMMAND ----------
owner = spark.sql("SELECT session_user() AS username").first().username
# Initial provisioning only: reruns never replace an intentionally restricted role.
spark.sql(f"""MERGE INTO {config.table('sec_user_access')} t USING
    (SELECT {sql_string(owner)} username, '*' wiki, true can_view_editor,
            'Lab 6 owner / job identity' access_reason, current_timestamp() updated_at) s
    ON t.username = s.username
    WHEN NOT MATCHED THEN INSERT *""")
result = owner

print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
