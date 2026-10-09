# Databricks notebook source
# MAGIC %md
# MAGIC # Provision Lab 7 tables and Delta constraints
# MAGIC Run separately before the quality Job; reruns preserve existing rows.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parameters and shared fact contract

# COMMAND ----------
import json
import sys

dbutils.widgets.text("code_root", "")
root = dbutils.widgets.get("code_root")
for path in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, f"{root}/{path}")
from lab7.config import notebook_config

config = notebook_config(dbutils.widgets)
spark.conf.set("spark.sql.session.timeZone", "UTC")

from lab7.constraints import FACT_CONSTRAINTS, FACT_NOT_NULL_COLUMNS, empty_fact_schema, fact_ddl

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the isolated schema and fact table

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.namespace}")
# Derive the contract from empty transformations; fact_candidate does not need to exist.
expected_schema = empty_fact_schema(spark)
target = config.table("fact_edits")
spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({fact_ddl(expected_schema)}) USING DELTA")
actual_schema = spark.table(target).schema
assert [(f.name, f.dataType) for f in actual_schema] == [
    (f.name, f.dataType) for f in expected_schema
], "Existing fact schema differs from the transformations; migrate it before refreshing"
assert FACT_NOT_NULL_COLUMNS <= {f.name for f in actual_schema if not f.nullable}, (
    "Existing fact table is missing NOT NULL constraints"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Install missing CHECK constraints without rewriting data

# COMMAND ----------
properties = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
for name, expression in FACT_CONSTRAINTS.items():
    if f"delta.constraints.{name}" not in properties:
        spark.sql(f"ALTER TABLE {target} ADD CONSTRAINT {name} CHECK ({expression})")
result = {"schema": config.namespace, "fact_table": target, "constraints": list(FACT_CONSTRAINTS)}
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
