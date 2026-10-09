# Databricks notebook source
import json
import sys

for name, default in {"catalog": "workspace", "schema": "ivanrazumovskyi_lab7",
                      "bronze_schema": "ivanrazumovskyi_lab5", "gold_schema": "ivanrazumovskyi_lab6", "code_root": ""}.items():
    dbutils.widgets.text(name, default)
root = dbutils.widgets.get("code_root")
for path in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, f"{root}/{path}")
from lab7.config import Config
from lab7.runtime import snapshot
config = Config(**{name: dbutils.widgets.get(name) for name in ("catalog", "schema", "bronze_schema", "gold_schema")})
dbutils.notebook.exit(json.dumps(snapshot(spark, config)))
