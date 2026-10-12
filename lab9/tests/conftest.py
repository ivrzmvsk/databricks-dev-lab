from types import SimpleNamespace as NS
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import CurrentUserAPI
from databricks.sdk.service.jobs import JobsAPI, NotebookOutput, RunOutput
from databricks.sdk.service.pipelines import PipelinesAPI

from lab9.auth import PERSONAL_HOST


@pytest.fixture
def client():
    client = create_autospec(WorkspaceClient, instance=True)
    client.config = NS(host=PERSONAL_HOST)
    client.jobs = create_autospec(JobsAPI, instance=True)
    client.pipelines = create_autospec(PipelinesAPI, instance=True)
    client.current_user = create_autospec(CurrentUserAPI, instance=True)
    client.current_user.me.return_value = NS(user_name="student@example.test")
    client.jobs.get.side_effect = [
        NS(settings=NS(name="lab9-wikipedia-setup-personal")),
        NS(settings=NS(name="lab9-wikipedia-gold-personal")),
    ]
    client.jobs.list_runs.side_effect = lambda **kwargs: iter([])
    client.pipelines.get.return_value = NS(name="lab9-wikipedia-personal", state="IDLE")
    client.jobs.run_now.return_value = NS(run_id=11)
    client.jobs.submit.return_value = NS(run_id=11)
    client.jobs.get_run_output.return_value = RunOutput(
        notebook_output=NotebookOutput(result='{"count": 42}')
    )
    client.pipelines.start_update.return_value = NS(update_id="update-1")
    client.pipelines.get_update.return_value = NS(update=NS(state="COMPLETED"))
    return client


@pytest.fixture
def inventory():
    return {
        "setup_job_id": 1,
        "gold_job_id": 2,
        "pipeline_id": "pipeline-1",
        "probe_path": "/Workspace/Users/student@example.test/.bundle/"
        "lab9_wikipedia_automation/personal/files/lab9/notebooks/compute_probe",
    }
