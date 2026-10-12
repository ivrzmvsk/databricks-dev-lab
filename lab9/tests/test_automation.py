import json
import signal
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace as NS

import pytest
from databricks.sdk.service.jobs import (
    Run,
    RunLifeCycleState,
    RunResultState,
    RunState,
    RunTask,
)

from lab9.automation import Automation
from lab9.preflight import resource_name_matches


def run(state="TERMINATED", result="SUCCESS", run_id=11):
    return Run(
        run_id=run_id,
        run_page_url="https://example.test/run",
        state=RunState(
            life_cycle_state=RunLifeCycleState(state),
            result_state=RunResultState(result) if result else None,
        ),
        tasks=[
            RunTask(
                task_key="check",
                run_id=12,
                state=RunState(
                    life_cycle_state=RunLifeCycleState(state),
                    result_state=RunResultState(result) if result else None,
                ),
            )
        ],
    )


@pytest.fixture
def setup(tmp_path, client, inventory):
    client.jobs.get_run.return_value = run()
    automation = Automation(client, inventory, tmp_path / "report.json", interval=1)
    return automation, client


def test_full_flow_starts_pipeline_once_and_collects_validation(setup):
    automation, client = setup
    assert automation.execute("run", "commit-sha") == 0
    assert [stage["kind"] for stage in automation.report["stages"]] == [
        "job",
        "pipeline",
        "job",
    ]
    assert [call.kwargs["job_id"] for call in client.jobs.run_now.call_args_list] == [
        1,
        2,
    ]
    client.pipelines.start_update.assert_called_once_with(
        pipeline_id="pipeline-1", full_refresh=False
    )
    assert automation.report["stages"][-1]["outputs"] == {
        "check": {"result": {"count": 42}}
    }
    assert json.loads(automation.path.read_text())["status"] == "SUCCESS"
    assert not client.clusters.mock_calls


@pytest.mark.parametrize(
    "state,result",
    [
        ("TERMINATED", "FAILED"),
        ("SKIPPED", None),
        ("INTERNAL_ERROR", None),
        ("TERMINATED", "CANCELED"),
    ],
)
def test_failed_setup_prevents_ingestion_and_gold(setup, state, result):
    automation, client = setup
    client.jobs.get_run.return_value = run(state, result)
    assert automation.execute("run") == 1
    client.jobs.run_now.assert_called_once()
    client.pipelines.start_update.assert_not_called()
    assert automation.report["status"] == "FAILED"


@pytest.mark.parametrize("state", ["FAILED", "CANCELED"])
def test_pipeline_failure_prevents_gold(setup, state):
    automation, client = setup
    client.pipelines.get_update.return_value = NS(update=NS(state=state))
    assert automation.execute("run") == 1
    client.jobs.run_now.assert_called_once()
    assert automation.report["stages"][-1]["state"] == state


def test_job_timeout_cancels_only_submitted_run(setup):
    automation, client = setup
    now = [0]
    automation.clock = lambda: now[0]
    automation.started = 0
    automation.timeout = 2
    automation.sleep = lambda seconds: now.__setitem__(0, now[0] + seconds)
    client.jobs.get_run.return_value = run("RUNNING", None)
    assert automation.execute("run") == 1
    client.jobs.cancel_run.assert_called_once_with(run_id=11)
    client.pipelines.stop.assert_not_called()
    assert automation.report["cleanup"] == [{"run_id": 11, "cancellation": "requested"}]


def test_cancellation_failure_preserves_original_error(setup):
    automation, client = setup
    client.jobs.get_run.side_effect = RuntimeError("monitor disconnected")
    client.jobs.cancel_run.side_effect = RuntimeError("cancel disconnected")
    assert automation.execute("run") == 1
    assert automation.report["error"] == "monitor disconnected"
    assert (
        automation.report["cleanup"][0]["cancellation_error"] == "cancel disconnected"
    )


def test_does_not_stop_a_different_pipeline_update(setup):
    automation, client = setup
    client.pipelines.get_update.side_effect = RuntimeError("connection lost")
    client.pipelines.get.side_effect = [
        NS(name="lab9-wikipedia-personal", state="IDLE"),
        NS(latest_updates=[NS(update_id="someone-else", state="RUNNING")]),
    ]
    assert automation.execute("pipeline") == 1
    client.pipelines.stop.assert_not_called()


def test_pipeline_timeout_requests_stop_for_owned_update(setup):
    automation, client = setup
    client.pipelines.get_update.side_effect = TimeoutError("deadline exceeded")
    client.pipelines.get.side_effect = [
        NS(name="lab9-wikipedia-personal", state="IDLE"),
        NS(latest_updates=[NS(update_id="update-1", state="RUNNING")]),
    ]
    assert automation.execute("pipeline") == 1
    client.pipelines.stop.assert_called_once_with(pipeline_id="pipeline-1")


def test_active_run_is_rejected_before_any_submission(setup):
    automation, client = setup
    client.jobs.list_runs.side_effect = lambda **kwargs: iter([NS(run_id=99)])
    assert automation.execute("run") == 1
    client.jobs.run_now.assert_not_called()
    client.pipelines.start_update.assert_not_called()
    client.jobs.cancel_run.assert_not_called()


def test_foreign_job_inventory_is_rejected(setup):
    automation, client = setup
    client.jobs.get.side_effect = [NS(settings=NS(name="lab8-existing-job"))]
    assert automation.execute("run") == 1
    client.jobs.run_now.assert_not_called()


def test_serverless_submit_has_no_classic_cluster_spec(setup):
    automation, client = setup
    assert automation.execute("submit-notebook") == 0
    call = client.jobs.submit.call_args.kwargs
    task = call["tasks"][0].as_dict()
    assert not {"new_cluster", "existing_cluster_id", "job_cluster_key"} & task.keys()
    assert call["timeout_seconds"] == 3600
    assert not {"compute", "classic_cluster_created"} & automation.report.keys()


@pytest.mark.parametrize("command", ["run", "pipeline", "submit-notebook"])
def test_report_does_not_claim_unmeasured_compute_type(setup, command):
    automation, _ = setup
    assert automation.execute(command) == 0
    report = json.loads(automation.path.read_text())
    assert report["format_version"] == 3
    assert not {"compute", "classic_cluster_created"} & report.keys()


def test_interrupt_records_cancellation(setup):
    automation, client = setup
    client.jobs.get_run.side_effect = KeyboardInterrupt
    assert automation.execute("run") == 1
    assert automation.report["status"] == "CANCELED"
    client.jobs.cancel_run.assert_called_once_with(run_id=11)


def test_sigterm_cancels_owned_run_and_restores_handler(setup):
    automation, client = setup
    previous = signal.getsignal(signal.SIGTERM)
    client.jobs.get_run.side_effect = lambda **kwargs: signal.raise_signal(
        signal.SIGTERM
    )
    assert automation.execute("run") == 1
    assert automation.report["status"] == "CANCELED"
    assert automation.report["error"] == "Received SIGTERM"
    client.jobs.cancel_run.assert_called_once_with(run_id=11)
    assert json.loads(automation.path.read_text())["status"] == "CANCELED"
    assert signal.getsignal(signal.SIGTERM) == previous


def test_task_result_does_not_overwrite_error(setup):
    automation, client = setup
    client.jobs.get_run.return_value = run(result="FAILED")
    client.jobs.get_run_output.return_value = NS(
        notebook_output=NS(result='{"count": 0}'),
        error="Validation failed",
        error_trace="trace details",
    )
    assert automation.execute("run") == 1
    assert automation.report["stages"][0]["outputs"]["check"] == {
        "error": "Validation failed",
        "trace": "trace details",
        "result": {"count": 0},
    }


def test_azure_client_rejected_without_api_calls(setup, tmp_path):
    _, client = setup
    client.config.host = "https://adb-7405618361895520.0.azuredatabricks.net"
    client.reset_mock()
    with pytest.raises(ValueError, match="restricted"):
        Automation(client, {}, tmp_path / "report.json")
    assert not client.mock_calls


def test_development_resource_prefix_matches_without_accepting_other_labs():
    assert resource_name_matches(
        "[dev student] lab9-wikipedia-personal", "lab9-wikipedia-personal"
    )
    assert not resource_name_matches(
        "[dev student] lab8-wikipedia-personal", "lab9-wikipedia-personal"
    )
    assert not resource_name_matches(
        "unrelated lab9-wikipedia-personal", "lab9-wikipedia-personal"
    )


def test_failed_task_error_is_saved_in_report(setup):
    automation, client = setup
    client.jobs.get_run.return_value = run(result="FAILED")
    client.jobs.get_run_output.return_value = NS(
        notebook_output=None,
        error="Wikipedia Silver is empty",
        error_trace="trace details",
    )
    assert automation.execute("run") == 1
    assert (
        automation.report["stages"][0]["outputs"]["check"]["error"]
        == "Wikipedia Silver is empty"
    )


def test_pipeline_can_be_retried_after_a_terminal_failure(setup):
    automation, client = setup
    client.pipelines.get.return_value = NS(
        name="lab9-wikipedia-personal", state="FAILED"
    )
    assert automation.execute("pipeline") == 0
    client.pipelines.start_update.assert_called_once()


def test_blocked_run_is_waited_for_not_treated_as_terminal(setup):
    automation, client = setup
    client.jobs.get_run.side_effect = [run("BLOCKED", None), run()]
    automation.sleep = lambda seconds: None
    assert automation.execute("submit-notebook") == 0
    assert client.jobs.get_run.call_count == 2
    client.jobs.cancel_run.assert_not_called()


def test_unknown_command_rejected_before_api_calls_or_report_write(setup):
    automation, client = setup
    with pytest.raises(ValueError, match="Unknown automation command"):
        automation.execute("unknown")
    assert not client.mock_calls
    assert not automation.path.exists()


def test_pipeline_poll_deadline_cancels_owned_update(setup):
    automation, client = setup
    now = [0]
    automation.clock = lambda: now[0]
    automation.started = 0
    automation.timeout = 2
    automation.sleep = lambda seconds: now.__setitem__(0, now[0] + seconds)
    client.pipelines.get_update.return_value = NS(update=NS(state="RUNNING"))
    client.pipelines.get.side_effect = [
        NS(name="lab9-wikipedia-personal", state="IDLE"),
        NS(latest_updates=[NS(update_id="update-1", state="RUNNING")]),
    ]
    assert automation.execute("pipeline") == 1
    assert client.pipelines.get_update.call_count == 2
    client.pipelines.stop.assert_called_once_with(pipeline_id="pipeline-1")
    assert automation.report["stages"][0]["state"] == "RUNNING"


def test_poll_emits_only_changed_pipeline_states(setup, capsys):
    automation, client = setup
    client.pipelines.get_update.side_effect = [
        NS(update=NS(state=state)) for state in ["RUNNING", "RUNNING", "COMPLETED"]
    ]
    automation.sleep = lambda seconds: None
    assert automation.execute("pipeline") == 0
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["state"] for event in events if "state" in event] == [
        "RUNNING",
        "COMPLETED",
    ]


def test_execution_from_worker_thread_does_not_change_signal_handler(setup):
    automation, _ = setup
    previous = signal.getsignal(signal.SIGTERM)
    with ThreadPoolExecutor(max_workers=1) as executor:
        assert (
            executor.submit(automation.execute, "submit-notebook").result(timeout=5)
            == 0
        )
    assert signal.getsignal(signal.SIGTERM) == previous


def test_explicit_trial_execution_uses_trial_resources_and_report(setup):
    from lab9.auth import TARGETS

    previous, client = setup
    client.config.host = TARGETS["azure_trial_2"]["host"]
    client.jobs.get.side_effect = [
        NS(settings=NS(name="lab9-wikipedia-setup-azure_trial_2")),
        NS(settings=NS(name="lab9-wikipedia-gold-azure_trial_2")),
    ]
    client.pipelines.get.return_value = NS(
        name="lab9-wikipedia-azure_trial_2", state="IDLE"
    )
    resources = dict(previous.inventory)
    resources["probe_path"] = resources["probe_path"].replace(
        "/personal/", "/azure_trial_2/"
    )
    automation = Automation(
        client, resources, previous.path, interval=1, target="azure_trial_2"
    )
    assert automation.execute("run") == 0
    assert automation.report["host"] == client.config.host
    assert automation.report["target"] == "azure_trial_2"
    assert not client.clusters.mock_calls
