# Databricks notebook source
# MAGIC %md
# MAGIC # Process Wikimedia events from Zerobus
# MAGIC Bronze contains deliveries from the external producer. This job rejects
# MAGIC conflicting events, merges one Silver row per event, and checks unique keys.
# MAGIC Schema creation and grants run separately through tools/setup_dev.py.

# COMMAND ----------
import re
from pathlib import Path
from string import Template

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
for name in (catalog, schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Expected a catalog or schema identifier")
bronze = f"`{catalog}`.`{schema}`.`bronze_wiki_events`"
silver = f"`{catalog}`.`{schema}`.`silver_wiki_events`"
sql_root = Path(dbutils.widgets.get("sql_root"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Reject conflicting source events
# MAGIC Each event ID must have the same business payload across all deliveries.
# MAGIC A conflict anywhere in Bronze stops the job before Silver is changed.

# COMMAND ----------
conflicts = spark.sql(
    f"""
    SELECT event_id FROM {bronze}
    GROUP BY event_id
    HAVING event_id IS NULL OR COUNT(DISTINCT to_json(struct(
        wiki, page_title, user_name, event_type, bytes_changed,
        is_bot, sequence_number, event_timestamp
    ))) > 1
    LIMIT 1
"""
).collect()
if conflicts:
    raise ValueError("Conflicting business payload for event_id")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Merge deliveries into Silver
# MAGIC The SQL resource below is also used by offline tests and acceptance tools.
# MAGIC It keeps the earliest delivery and counts duplicates. Repeating the MERGE
# MAGIC on unchanged Bronze does not change Silver.

# COMMAND ----------
merge_sql = Template((sql_root / "01_merge.sql").read_text()).substitute(bronze=bronze, silver=silver)
spark.sql(merge_sql).collect()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Validate and show the result

# COMMAND ----------
duplicates = spark.sql(
    f"SELECT event_id FROM {silver} GROUP BY event_id HAVING COUNT(*) > 1 LIMIT 1"
).collect()
if duplicates:
    raise ValueError("Duplicate event_id in Silver")
count = spark.sql(f"SELECT COUNT(*) FROM {silver}").collect()[0][0]
print({"silver_events": count, "unique": True})
display(spark.sql(f"SELECT * FROM {silver} ORDER BY event_id LIMIT 20"))
