import subprocess
from unittest.mock import Mock

import pytest

from lab9 import deployment


@pytest.mark.parametrize(
    "action,extra",
    [
        ("validate", ["--strict"]),
        ("deploy", []),
        ("summary", ["--output", "json"]),
    ],
)
def test_bundle_commands_are_personal_and_bounded(monkeypatch, action, extra):
    execute = Mock(return_value="result")
    monkeypatch.setattr(subprocess, "check_output", execute)
    environment = {"DATABRICKS_TOKEN": "synthetic-token"}
    assert deployment.bundle_command(action, environment) == "result"
    execute.assert_called_once_with(
        [
            "databricks",
            "bundle",
            action,
            "--target",
            "personal",
            "--profile",
            "personal-env",
            *extra,
        ],
        cwd=deployment.ROOT,
        env=environment,
        text=True,
        timeout=1200,
    )


def test_unsupported_bundle_operation_cannot_start_a_process(monkeypatch):
    execute = Mock()
    monkeypatch.setattr(subprocess, "check_output", execute)
    with pytest.raises(ValueError, match="Unsupported bundle operation"):
        deployment.bundle_command("run", {})
    execute.assert_not_called()


def test_explicit_trial_bundle_command_uses_trial_profile(monkeypatch):
    execute = Mock(return_value="result")
    monkeypatch.setattr(subprocess, "check_output", execute)
    deployment.bundle_command("validate", {}, "azure_trial_2")
    command = execute.call_args.args[0]
    assert command[3:] == [
        "--target",
        "azure_trial_2",
        "--profile",
        "azure_trial_2",
        "--strict",
    ]
