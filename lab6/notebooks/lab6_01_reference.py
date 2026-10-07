# Databricks notebook source
# MAGIC %md
# MAGIC # Load Wikimedia references; setup is a separate Job

# COMMAND ----------
import json
import sys
from pathlib import Path
from pyspark.sql import functions as F

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets, widget_bool
from lab6.tables import ANALYTICAL_TABLES, replace_table
from lab6.security import assert_policies
from lab6.metadata import fetch_snapshot
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Check bootstrap mode and existing policies

# COMMAND ----------
bootstrap = widget_bool(dbutils, 'bootstrap_mode')
if bootstrap:
    existing = [t for t in ANALYTICAL_TABLES if spark.catalog.tableExists(config.table(t))]
    assert not existing, 'Bootstrap is only for an empty Gold schema; use a regular refresh for existing tables'
else:
    # Fail before changing any data if expected policies are missing.
    print(json.dumps(assert_policies(spark, config)))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch public metadata or read the existing Volume snapshot

# COMMAND ----------
if config.refresh_reference:
    observed = {r.wiki for r in spark.table(config.source).select('wiki').distinct().collect()}
    snapshot = fetch_snapshot(observed)
    try:
        path = Path(config.metadata_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(snapshot, ensure_ascii=False))
    except OSError:
        pass
else:
    snapshot = json.loads(Path(config.metadata_path).read_text())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load the reference Delta table

# COMMAND ----------
rows = []
for site in snapshot['sites']:
    rows.append(tuple(site.get(k) for k in [
        'wiki', 'language_code', 'language_name', 'project_code', 'site_name',
        'site_url', 'is_closed', 'is_private', 'raw_json',
    ]) + (json.dumps(snapshot.get('namespaces', {}).get(site['wiki'], {}), ensure_ascii=False),))
schema = ('wiki STRING, language_code STRING, language_name STRING, project_code STRING, '
          'site_name STRING, site_url STRING, is_closed BOOLEAN, is_private BOOLEAN, '
          'raw_json STRING, namespaces_json STRING')
frame = spark.createDataFrame(rows, schema).withColumn('fetched_at', F.current_timestamp())
assert frame.select('wiki').distinct().count() == frame.count(), 'Duplicate SiteMatrix wiki IDs'
replace_table(spark, config, 'ref_sitematrix_raw', frame)
result = {'sites': len(rows), 'namespace_errors': snapshot.get('namespace_errors', {})}

print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
