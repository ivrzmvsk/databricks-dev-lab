# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Gold data and preserved governance

# COMMAND ----------
import json
import sys
from pathlib import Path
from pyspark.sql import functions as F

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets, widget_bool
from lab6.tables import ALL_TABLES
from lab6.security import assert_policies
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fact uniqueness and reconciliation to pinned Silver

# COMMAND ----------
fact = spark.table(config.table('fact_edits'))
n = fact.count()
assert n > 0
assert fact.select('event_id').distinct().count() == n, 'Duplicate events in fact'
version = spark.sql(f"SHOW TBLPROPERTIES {config.table('fact_edits')} ('lab6.source_version')").first()[1]
source_n = spark.read.option('versionAsOf', int(version)).table(config.source).select('event_id').distinct().count()
assert source_n == n, 'Gold count differs from the pinned Silver snapshot'

# COMMAND ----------
# MAGIC %md
# MAGIC ## Dimension keys and foreign-key completeness

# COMMAND ----------
for table, key in [('dim_wiki','wiki_key'),('dim_date','date_key'),
                   ('dim_namespace','namespace_key'),('dim_page','page_key'),('dim_editor','editor_key')]:
    dim = spark.table(config.table(table))
    assert dim.count() == dim.select(key).distinct().count(), f'Duplicate {table} keys'
    assert dim.filter(F.col(key).isNull()).count() == 0
    assert fact.join(dim.select(key), key, 'left_anti').count() == 0, f'Orphan {key}'

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregate totals and byte arithmetic

# COMMAND ----------
for table in ['agg_wiki_5min', 'agg_wiki_daily', 'agg_page_activity']:
    agg = spark.table(config.table(table))
    assert agg.agg(F.sum('edit_count')).first()[0] == n, f'{table} count mismatch'
    assert agg.agg(F.sum('net_bytes_delta')).first()[0] == fact.agg(F.sum('bytes_delta')).first()[0]
assert fact.filter('bytes_delta IS NOT NULL AND bytes_delta <> bytes_added - bytes_removed').count() == 0

# COMMAND ----------
# MAGIC %md
# MAGIC ## Report table counts and metadata coverage

# COMMAND ----------
counts = {t: spark.table(config.table(t)).count() for t in ALL_TABLES}
result = {'counts': counts, 'source_unique_events': source_n,
            'unmatched_wikis': spark.table(config.table('dim_wiki')).filter('NOT metadata_matched').count(),
            'unmatched_namespaces': spark.table(config.table('dim_namespace')).filter('NOT metadata_matched').count()}


# COMMAND ----------
# MAGIC %md
# MAGIC ## Read-only policy verification; never reapply governance

# COMMAND ----------
bootstrap = widget_bool(dbutils, 'bootstrap_mode')
result['policies'] = {'status': 'deferred_until_governance', 'bootstrap': True} if bootstrap else assert_policies(spark, config)

print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
