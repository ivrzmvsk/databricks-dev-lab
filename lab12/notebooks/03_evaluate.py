# Databricks notebook source
# ruff: noqa: F821, E402
# MAGIC %pip install databricks-sdk==0.152.0 tiktoken==0.14.0

# COMMAND ----------
# MAGIC %pip install mlflow[databricks]==3.17.1 databricks-agents==1.12.0

# COMMAND ----------
import json
import sys
from pathlib import Path

root = Path(dbutils.widgets.get("code_root"))
sys.path.insert(0, str(root / "src"))

import hashlib
import os
import time
from dataclasses import asdict

import mlflow

from lab12.evaluation import evaluation_records, get_or_create_dataset, run_evaluation
from lab12.notebook import initialize
from lab12.observability import configure, verify_trace
from lab12.qa import Assistant, ServingModel
from lab12.release import automated_gate, build_hash
from lab12.retrieval import VectorRetriever

initial_build = dbutils.jobs.taskValues.get(taskKey="ingest", key="build_hash")
context = initialize(root, dbutils, expected_build=initial_build)
settings, workspace = context.settings, context.workspace
documents, chunks = context.documents, context.chunks
path = root / "dataset/board_games.json"
corpus_hash = hashlib.sha256(path.read_bytes()).hexdigest()

# COMMAND ----------
# Configure governed tracing and verify a real retrieval/generation trace.
os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "1"
# Keep native code scorers sequential on the shared personal workspace.
os.environ["MLFLOW_GENAI_EVAL_MAX_SCORER_WORKERS"] = "1"
experiment_id = configure(
    workspace, settings, dbutils.widgets.get("experiment_name"), dbutils.widgets.get("warehouse_id")
)
assistant = Assistant(
    VectorRetriever(workspace, settings),
    ServingModel(workspace, settings),
    settings,
    tracing=True,
)
started_ms = int(time.time() * 1000)
smoke = assistant.ask("How many players can play Moon Station?", {"doc_id": "BG01"})
trace = verify_trace(
    experiment_id, settings.host, started_ms, mlflow.get_last_active_trace_id(thread_local=True)
)

# COMMAND ----------
# Compare eight fixed questions with and without retrieval.
records = evaluation_records(documents)
dataset = get_or_create_dataset(settings, experiment_id, records, corpus_hash)
rag = run_evaluation(assistant, dataset, "rag")
baseline = run_evaluation(assistant, dataset, "baseline")
if build_hash(root) != initial_build:
    raise ValueError("Source files changed during execution; rerun the frozen build")

# COMMAND ----------
# Record functional evidence; factual answer review remains separate.
report = {
    "build_hash": initial_build,
    "corpus_hash": corpus_hash,
    "experiment_id": experiment_id,
    "trace": trace,
    "smoke": asdict(smoke),
    "rag": rag,
    "baseline": baseline,
    "dev_passed": False,
    "manual_factual_review_required": True,
}
with mlflow.start_run(run_name="board-games-release-report"):
    mlflow.log_dict(report, "comparison.json")
    report["report_run_id"] = mlflow.active_run().info.run_id
    automated_gate(rag)
    if baseline["errors"] or baseline["completed"] != 8:
        raise ValueError("Baseline evaluation has failed or missing answers")
    report["dev_passed"] = settings.target == "dev"
    mlflow.log_dict(report, "comparison.json")
dbutils.notebook.exit(json.dumps(report, ensure_ascii=False))
