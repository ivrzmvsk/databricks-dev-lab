# Databricks notebook source
# ruff: noqa: F821, E402
# MAGIC %pip install databricks-sdk==0.152.0 tiktoken==0.14.0

# COMMAND ----------
import json
import sys
from pathlib import Path

root = Path(dbutils.widgets.get("code_root"))
sys.path.insert(0, str(root / "src"))

from lab12.index import sync_index
from lab12.notebook import initialize
from lab12.release import build_hash
from lab12.retrieval import VectorRetriever

initial_build = dbutils.jobs.taskValues.get(taskKey="ingest", key="build_hash")
context = initialize(root, dbutils, expected_build=initial_build)
settings, workspace = context.settings, context.workspace
documents, chunks = context.documents, context.chunks

# COMMAND ----------
# Synchronize an existing index only when its complete snapshot differs.
result = sync_index(workspace, settings, len(chunks), expected_chunks=chunks)

# COMMAND ----------
# Verify real metadata filtering and indexed source freshness.
retriever = VectorRetriever(workspace, settings)
by_id = {c["chunk_id"]: c for c in chunks}
for doc_id in ("BG01", "BG09", "BG12"):
    hits = retriever.search("Describe this board game's rules", {"doc_id": doc_id})
    if not hits or any(
        h.doc_id != doc_id
        or h.chunk_id not in by_id
        or h.chunk_text != by_id[h.chunk_id]["chunk_text"]
        for h in hits
    ):
        raise ValueError(f"Index freshness or filter validation failed for {doc_id}")
if build_hash(root) != initial_build:
    raise ValueError("Source changed during index synchronization")
dbutils.notebook.exit(json.dumps(result))
