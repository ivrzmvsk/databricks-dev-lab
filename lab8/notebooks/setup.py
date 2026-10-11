# Databricks notebook source
# MAGIC %md
# MAGIC # Prepare Lab 8 from versioned DDL and reference metadata
# MAGIC Schemas are bundle resources. This task never touches Lab 5/6 tables.

# COMMAND ----------
import hashlib
import json
import sys
from pathlib import Path

dbutils.widgets.text("code_root", "")
root = Path(dbutils.widgets.get("code_root"))
for path in ("lab6/src", "lab8/src"):
    sys.path.insert(0, str(root / path))
from lab6.config import from_widgets
from lab6.tables import sql_string
from lab8.migrations import pending_migrations, split_sql

config = from_widgets(dbutils)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Checksum applied migrations; add new versions instead of editing old ones

# COMMAND ----------
ledger = config.namespace + ".ops_schema_migrations"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {ledger} (
    migration STRING NOT NULL, checksum STRING NOT NULL, applied_at TIMESTAMP
) USING DELTA""")
applied = {r.migration: r.checksum for r in spark.table(ledger).collect()}
for name, checksum, content in pending_migrations(
    list((root / "lab8/sql/migrations").glob("*.sql")), applied
):
    for statement in split_sql(content.replace("{namespace}", config.namespace)):
        spark.sql(statement)
    spark.sql(f"""MERGE INTO {ledger} t USING (
        SELECT {sql_string(name)} migration, {sql_string(checksum)} checksum,
               current_timestamp() applied_at) s
        ON t.migration = s.migration WHEN NOT MATCHED THEN INSERT *""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Deploy the checked-in reference snapshot, without outbound API requests

# COMMAND ----------
spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.namespace}.lab8_assets")
source = root / "lab8/assets/wikimedia.json"
target = Path(config.metadata_path)
target.parent.mkdir(parents=True, exist_ok=True)
content = source.read_bytes()
if not target.exists() or target.read_bytes() != content:
    target.write_bytes(content)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reconcile access mappings from code; reader names remain masked by default

# COMMAND ----------
dbutils.widgets.text("reader_principal", "")
reader = dbutils.widgets.get("reader_principal")
owner = spark.sql("SELECT session_user()").first()[0]
policy = json.loads((root / "lab8/assets/access_policy.json").read_text())
rows = [(owner, "*", True, "Lab 8 execution identity")]
if reader and reader != owner:
    rows.extend(
        (reader, wiki, policy["reader_can_view_editor"], "Lab 8 code-managed reader")
        for wiki in policy["reader_wikis"]
    )
access = spark.createDataFrame(
    rows, "username STRING, wiki STRING, can_view_editor BOOLEAN, access_reason STRING"
)
access.createOrReplaceTempView("_lab8_access")
spark.sql(f"""MERGE INTO {config.table("sec_user_access")} t USING _lab8_access s
    ON t.username = s.username AND t.wiki = s.wiki
    WHEN MATCHED THEN UPDATE SET t.can_view_editor = s.can_view_editor,
        t.access_reason = s.access_reason, t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT
        (username, wiki, can_view_editor, access_reason, updated_at)
        VALUES (s.username, s.wiki, s.can_view_editor, s.access_reason, current_timestamp())
    WHEN NOT MATCHED BY SOURCE THEN DELETE""")
result = {
    "schema": config.namespace,
    "metadata_sha256": hashlib.sha256(content).hexdigest(),
    "execution_identity": owner,
    "reader": reader,
}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
