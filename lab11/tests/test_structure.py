"""Check environment isolation and the actual job notebook's execution order."""

import runpy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from _acceptance import AcceptanceEnvironment

from lab11 import common

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("target", ["personal", "azure_trial_2"])
def test_checks_use_selected_warehouse_and_client(monkeypatch, target):
    env = AcceptanceEnvironment(target)
    env.client = Mock()
    execute = Mock(return_value=[])
    monkeypatch.setattr(common, "sql", execute)
    env.execute("SELECT 1")
    execute.assert_called_once_with(env.client, "SELECT 1", warehouse=common.TARGETS[target]["warehouse"])


def test_processing_cannot_cross_target_schema():
    env = AcceptanceEnvironment("azure_trial_2")
    env.execute = Mock()
    with pytest.raises(ValueError, match="outside Lab 11"):
        env.process(common.BRONZE, env.silver)
    env.execute.assert_not_called()


@pytest.mark.parametrize("conflicting", [False, True])
def test_job_notebook_checks_conflicts_before_merge(conflicting):
    queries = []

    def sql(statement):
        queries.append(statement)
        if "HAVING event_id IS NULL" in statement:
            rows = [["conflict"]] if conflicting else []
        elif "SELECT COUNT(*)" in statement:
            rows = [[3]]
        else:
            rows = []
        return SimpleNamespace(collect=lambda: rows)

    widgets = {
        "sql_root": str(ROOT / "src/lab11/sql"),
        "catalog": "workspace",
        "schema": "ivanrazumovskyi_lab11_zerobus",
    }
    namespace = {
        "dbutils": SimpleNamespace(widgets=SimpleNamespace(get=widgets.__getitem__)),
        "spark": SimpleNamespace(sql=sql),
        "display": Mock(),
    }
    if conflicting:
        with pytest.raises(ValueError, match="Conflicting business payload"):
            runpy.run_path(str(ROOT / "notebooks/01_process.py"), init_globals=namespace)
        assert len(queries) == 1
    else:
        runpy.run_path(str(ROOT / "notebooks/01_process.py"), init_globals=namespace)
        assert "HAVING event_id IS NULL" in queries[0]
        assert queries[1].lstrip().startswith("MERGE")
        assert "HAVING COUNT(*) > 1" in queries[2]
        namespace["display"].assert_called_once()


def test_azure_dispatcher_passes_context_without_mutating_dev(monkeypatch):
    runner = runpy.run_path(str(ROOT / "tools/check_azure.py"))
    scenario = Mock()
    monkeypatch.setitem(runner["SCENARIOS"], "validate", scenario)
    runner["main"](["validate", "--manifest", "example.json", "--min-deliveries", "120"])
    args, kwargs = scenario.call_args
    assert args[0] == ["--manifest", "example.json", "--min-deliveries", "120"]
    assert kwargs["environment"].target == "azure_trial_2"
    assert AcceptanceEnvironment().target == "personal"
    assert common.BRONZE.startswith(common.DEV["schema"] + ".")
