import fcntl
import json
import subprocess
from unittest.mock import Mock

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.jobs import Run, RunLifeCycleState, RunResultState, RunState

from lab9 import cli
from lab9.auth import BUNDLE_NAME, PERSONAL_HOST


@pytest.fixture
def local_cli(tmp_path, monkeypatch, client, inventory):
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", PERSONAL_HOST)
    monkeypatch.setenv("DATABRICKS_TOKEN", "synthetic-token")
    monkeypatch.setattr(cli, "workspace_client", lambda environment, target: client)
    client.jobs.get_run.return_value = Run(
        run_id=11,
        state=RunState(
            life_cycle_state=RunLifeCycleState.TERMINATED,
            result_state=RunResultState.SUCCESS,
        ),
    )
    path = tmp_path / "inventory.json"
    path.write_text(
        json.dumps(
            {
                **inventory,
                "bundle": BUNDLE_NAME,
                "target": "personal",
                "host": PERSONAL_HOST,
            }
        )
    )
    return ["--inventory", str(path)]


def test_status_prints_sdk_state(local_cli, client, capsys):
    assert cli.main([*local_cli, "status", "11"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["run_id"] == 11
    assert result["state"] == "TERMINATED"
    assert result["result"] == "SUCCESS"
    client.jobs.get_run.assert_called_once_with(run_id=11)
    client.current_user.me.assert_not_called()


@pytest.mark.parametrize("error", [NotFound, PermissionDenied])
def test_status_sdk_errors_are_one_line(local_cli, client, capsys, error):
    client.jobs.get_run.side_effect = error("Cannot read run\nAdditional details")
    assert cli.main([*local_cli, "status", "999"]) == 1
    captured = capsys.readouterr()
    assert captured.err == "Lab 9: Cannot read run Additional details\n"
    assert captured.out == ""


def test_changed_summary_format_is_a_cli_error(local_cli, monkeypatch, capsys):
    monkeypatch.setattr(cli, "bundle_command", lambda *args: '{"unexpected": true}')
    assert cli.main([*local_cli, "inventory"]) == 1
    assert capsys.readouterr().err == "Lab 9: 'bundle'\n"


def test_bundle_timeout_is_a_cli_error(local_cli, monkeypatch, capsys):
    bundle = Mock(side_effect=subprocess.TimeoutExpired("databricks", 1200))
    monkeypatch.setattr(cli, "bundle_command", bundle)
    assert cli.main([*local_cli, "validate"]) == 1
    assert "timed out after 1200 seconds" in capsys.readouterr().err


@pytest.mark.parametrize(
    "command,stages",
    [
        ("run", ["job", "pipeline", "job"]),
        ("pipeline", ["pipeline"]),
        ("submit-notebook", ["submitted_notebook"]),
    ],
)
def test_execution_routes_save_real_reports(local_cli, tmp_path, command, stages):
    path = tmp_path / "report.json"
    assert (
        cli.main(
            [*local_cli, command, "--report", str(path), "--commit", "test-commit"]
        )
        == 0
    )
    report = json.loads(path.read_text())
    assert report["status"] == "SUCCESS"
    assert report["command"] == command
    assert [stage["kind"] for stage in report["stages"]] == stages


def test_lock_blocks_submission_and_releases_after_holder_exits(
    local_cli, tmp_path, client
):
    path = tmp_path / ".state/automation.lock"
    path.parent.mkdir()
    with path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert cli.main([*local_cli, "submit-notebook"]) == 1
        client.jobs.submit.assert_not_called()
        client.current_user.me.assert_not_called()
    assert cli.main([*local_cli, "submit-notebook"]) == 0
    client.jobs.submit.assert_called_once()


@pytest.mark.parametrize(
    "command,expected",
    [
        ("validate", ["validate"]),
        ("inventory", ["summary"]),
        ("deploy", ["validate", "deploy", "summary"]),
    ],
)
def test_bundle_routes_and_inventory(
    local_cli, tmp_path, monkeypatch, command, expected
):
    summary = {
        "bundle": {"name": BUNDLE_NAME, "target": "personal"},
        "workspace": {"host": PERSONAL_HOST, "file_path": "/Workspace/test/files"},
        "resources": {
            "jobs": {"setup_job": {"id": "101"}, "gold_job": {"id": "102"}},
            "pipelines": {"wiki_pipeline": {"id": "pipeline-new"}},
        },
    }
    bundle = Mock(
        side_effect=lambda action, environment, target: (
            json.dumps(summary) if action == "summary" else "OK"
        )
    )
    monkeypatch.setattr(cli, "bundle_command", bundle)
    assert cli.main([*local_cli, command]) == 0
    assert [call.args[0] for call in bundle.call_args_list] == expected
    if command != "validate":
        saved = json.loads((tmp_path / "inventory.json").read_text())
        assert saved["setup_job_id"] == 101
        assert saved["gold_job_id"] == 102
        assert saved["pipeline_id"] == "pipeline-new"
