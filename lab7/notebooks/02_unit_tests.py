# Databricks notebook source
import builtins
import contextlib
import io
import json
import os
import sys
from pathlib import Path
import pytest

dbutils.widgets.text("code_root", "")
root = Path(dbutils.widgets.get("code_root"))
for path in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, str(root / path))
os.environ["LAB7_SPARK_BACKEND"] = "runtime"
os.environ["TZ"] = "UTC"
builtins._lab7_spark = spark
# Workspace files are read-only for bytecode/cache creation on serverless.
sys.dont_write_bytecode = True
output = io.StringIO()
with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
    exit_code = pytest.main([str(root / "lab7/tests"), "-c", str(root / "lab7/pytest.ini"), "-q", "-p", "no:cacheprovider"])
if exit_code:
    raise AssertionError(f"pytest failed with exit code {exit_code}: {output.getvalue()[-12000:]}")
dbutils.notebook.exit(json.dumps({"pytest_exit_code": int(exit_code), "backend": "runtime", "output": output.getvalue()}))
