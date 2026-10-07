# Databricks notebook source
# MAGIC %md
# MAGIC # Configure RLS, CLS and grants; run separately from refresh

# COMMAND ----------
import json
import sys
from pathlib import Path

dbutils.widgets.text("source_code_path", "")
source_code_path = dbutils.widgets.get("source_code_path") or str(Path.cwd().parent / "src")
sys.path.insert(0, source_code_path)
from lab6.config import from_widgets
from lab6.tables import ANALYTICAL_TABLES, WIKI_TABLES
from lab6.security import assert_policies
config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read the analytical reader principal

# COMMAND ----------
dbutils.widgets.text('reader_principal', '')
reader = dbutils.widgets.get('reader_principal')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the row-filter and column-mask functions

# COMMAND ----------
access = config.table('sec_user_access')
spark.sql(f"""CREATE OR REPLACE FUNCTION {config.namespace}.wiki_access(row_wiki STRING)
    RETURNS BOOLEAN RETURN EXISTS (SELECT 1 FROM {access} a
    WHERE a.username = session_user() AND (a.wiki = row_wiki OR a.wiki = '*'))""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {config.namespace}.editor_mask(value STRING, row_wiki STRING)
    RETURNS STRING RETURN CASE WHEN EXISTS (SELECT 1 FROM {access} a
    WHERE a.username = session_user() AND a.can_view_editor
    AND (a.wiki = row_wiki OR a.wiki = '*')) THEN value ELSE '[MASKED]' END""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Attach policies to the analytical tables

# COMMAND ----------
for table in WIKI_TABLES:
    spark.sql(f"ALTER TABLE {config.table(table)} SET ROW FILTER {config.namespace}.wiki_access ON (wiki)")
spark.sql(f"ALTER TABLE {config.table('dim_editor')} ALTER COLUMN user_name "
          f"SET MASK {config.namespace}.editor_mask USING COLUMNS (wiki)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Grant analytical access only after policies are installed

# COMMAND ----------
if reader:
    principal = '`' + reader.replace('`', '``') + '`'
    session_user = spark.sql('SELECT session_user()').first()[0]
    # The job identity already uses this catalog; a redundant self-grant would
    # require catalog MANAGE even when it owns every Lab 6 object.
    if reader != session_user:
        spark.sql(f'GRANT USE CATALOG ON CATALOG {config.catalog} TO {principal}')
    spark.sql(f'GRANT USE SCHEMA ON SCHEMA {config.namespace} TO {principal}')
    for table in ANALYTICAL_TABLES:
        spark.sql(f'GRANT SELECT ON TABLE {config.table(table)} TO {principal}')
    # No SELECT/MODIFY on raw references, access mappings, or operational logs.
result = {'row_filters': list(WIKI_TABLES), 'masked_column': 'dim_editor.user_name', 'reader': reader}

result['policy_check'] = assert_policies(spark, config)

print(json.dumps(result, default=str))
dbutils.notebook.exit(json.dumps(result, default=str))
