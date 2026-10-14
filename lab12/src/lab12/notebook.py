"""Shared initialization only; task orchestration stays in each notebook."""

from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient

from lab12.config import Settings
from lab12.corpus import prepare
from lab12.release import build_hash, require_release


@dataclass
class NotebookContext:
    settings: Settings
    workspace: WorkspaceClient
    build: str
    documents: list
    chunks: list


def initialize(root: Path, dbutils, *, expected_build=None):
    settings = Settings.from_widgets(dbutils)
    workspace = WorkspaceClient()
    settings.assert_host(workspace.config.host)
    if settings.target == "prod":
        require_release(root)
    build = build_hash(root)
    if expected_build is not None and build != expected_build:
        raise ValueError("Source differs from the ingestion task")
    documents, chunks = prepare(root / "dataset/board_games.json")
    return NotebookContext(settings, workspace, build, documents, chunks)
