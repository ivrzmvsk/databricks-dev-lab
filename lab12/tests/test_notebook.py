from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lab12 import notebook
from lab12.config import Settings


def setup_context(monkeypatch, target="dev"):
    settings = (
        Settings()
        if target == "dev"
        else Settings(
            target="prod", catalog="dbr_dev", schema="ivanrazumovskyi_lab12_board_games_prod"
        )
    )
    workspace = MagicMock()
    workspace.config.host = settings.host
    monkeypatch.setattr(notebook.Settings, "from_widgets", lambda _: settings)
    monkeypatch.setattr(notebook, "WorkspaceClient", lambda: workspace)
    monkeypatch.setattr(notebook, "build_hash", lambda _: "build")
    prepare = MagicMock(return_value=(["document"], ["chunk"]))
    monkeypatch.setattr(notebook, "prepare", prepare)
    return workspace, prepare


def test_initialization_does_not_execute_tasks(monkeypatch):
    workspace, prepare = setup_context(monkeypatch)
    context = notebook.initialize(Path("/local"), MagicMock(), expected_build="build")
    assert context.build == "build"
    assert context.documents == ["document"] and context.chunks == ["chunk"]
    prepare.assert_called_once_with(Path("/local/dataset/board_games.json"))
    assert workspace.mock_calls == []


def test_changed_build_rejected_before_data_preparation(monkeypatch):
    _, prepare = setup_context(monkeypatch)
    with pytest.raises(ValueError, match="Source differs"):
        notebook.initialize(Path("/local"), MagicMock(), expected_build="old")
    prepare.assert_not_called()


def test_wrong_host_rejected_before_data_preparation(monkeypatch):
    workspace, prepare = setup_context(monkeypatch)
    workspace.config.host = "https://wrong-workspace.example"
    with pytest.raises(ValueError):
        notebook.initialize(Path("/local"), MagicMock())
    prepare.assert_not_called()


def test_prod_requires_release_before_data_preparation(monkeypatch):
    _, prepare = setup_context(monkeypatch, target="prod")
    guard = MagicMock(side_effect=ValueError("not approved"))
    monkeypatch.setattr(notebook, "require_release", guard)
    with pytest.raises(ValueError, match="not approved"):
        notebook.initialize(Path("/local"), MagicMock())
    guard.assert_called_once_with(Path("/local"))
    prepare.assert_not_called()
