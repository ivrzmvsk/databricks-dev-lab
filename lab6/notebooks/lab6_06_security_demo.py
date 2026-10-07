# Databricks notebook source
# MAGIC %md
# MAGIC # Verify real RLS/CLS and restore original access in finally

# COMMAND ----------
import json
import sys
from pathlib import Path
from pyspark.sql import functions as F

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets
from lab6.tables import WIKI_TABLES, sql_string
from lab6.security import assert_policies
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify policies before changing access mappings

# COMMAND ----------
assert_policies(spark, config)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save the current mapping and expected row counts

# COMMAND ----------
username = spark.sql('SELECT session_user()').first()[0]
access = config.table('sec_user_access')
original = spark.table(access).filter(F.col('username') == username).collect()
total = spark.table(config.table('fact_edits')).count()
selected = spark.table(config.table('fact_edits')).groupBy('wiki').count().orderBy(F.col('count').desc()).first().wiki
expected = spark.table(config.table('fact_edits')).filter(F.col('wiki') == selected).count()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Restrict to one wiki, mask editor names, then verify default-deny

# COMMAND ----------
try:
    spark.sql(f'DELETE FROM {access} WHERE username = {sql_string(username)}')
    spark.sql(f"INSERT INTO {access} VALUES ({sql_string(username)}, {sql_string(selected)}, false, 'Temporary enforcement test', current_timestamp())")
    for table in WIKI_TABLES:
        rows = spark.table(config.table(table))
        assert rows.filter(F.col('wiki') != selected).count() == 0, f'RLS failed on {table}'
    restricted = spark.table(config.table('fact_edits')).count()
    assert restricted == expected
    assert spark.table(config.table('dim_editor')).filter("user_name <> '[MASKED]'").count() == 0
    masked = spark.table(config.table('dim_editor')).count()
    spark.sql(f'DELETE FROM {access} WHERE username = {sql_string(username)}')
    assert spark.table(config.table('fact_edits')).count() == 0, 'RLS must deny unmapped users'
    result = {'method': 'Real session identity, temporary access mapping; not a second-user test',
              'allowed_wiki': selected, 'full_rows': total, 'restricted_rows': restricted,
              'masked_editors': masked, 'unmapped_rows': 0, 'tables_checked': list(WIKI_TABLES)}
finally:
    spark.sql(f'DELETE FROM {access} WHERE username = {sql_string(username)}')
    if original:
        spark.createDataFrame(original, spark.table(access).schema).write.mode('append').saveAsTable(access)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Confirm restored owner visibility

# COMMAND ----------
assert spark.table(config.table('fact_edits')).count() == total, 'Owner access was not restored'

print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
