# Databricks notebook source
import json
import sys

for name, default in {"catalog": "workspace", "schema": "ivanrazumovskyi_lab7",
                      "bronze_schema": "ivanrazumovskyi_lab5", "gold_schema": "ivanrazumovskyi_lab6", "code_root": ""}.items():
    dbutils.widgets.text(name, default)
root = dbutils.widgets.get("code_root")
dbutils.widgets.text("snapshot_max_age_seconds", "")
age_setting = dbutils.widgets.get("snapshot_max_age_seconds").strip()
snapshot_max_age_seconds = int(age_setting) if age_setting else None
for path in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, f"{root}/{path}")
from lab7.config import Config
from lab7.runtime import publish_constrained_fact, run_suite
from lab7.execution import build_dqx_engine
from databricks.sdk import WorkspaceClient
config = Config(**{name: dbutils.widgets.get(name) for name in ("catalog", "schema", "bronze_schema", "gold_schema")})
constraints = publish_constrained_fact(spark, config)
result = run_suite(spark, config, build_dqx_engine(spark, WorkspaceClient()),
                   snapshot_max_age_seconds=snapshot_max_age_seconds)
result["constraints"] = constraints
dbutils.notebook.exit(json.dumps(result, default=str))
