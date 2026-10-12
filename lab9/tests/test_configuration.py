from pathlib import Path

import yaml

from lab9.auth import PERSONAL_HOST
from lab9.preflight import JOB_NAMES, PIPELINE_NAME

ROOT = Path(__file__).resolve().parents[1]


def test_resources_are_isolated_and_unscheduled():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert set(bundle["targets"]) == {"personal", "azure_trial_2"}
    assert "lab9" in bundle["bundle"]["name"]
    for key in ("silver_schema", "gold_schema"):
        assert "_lab9_" in bundle["variables"][key]["default"]
    trial = bundle["targets"]["azure_trial_2"]
    lab8 = yaml.safe_load((ROOT.parent / "lab8/databricks.yml").read_text())
    assert trial["workspace"]["host"] == lab8["targets"]["prod"]["workspace"]["host"]
    assert trial["workspace"]["profile"] == "azure_trial_2"
    assert trial["variables"]["catalog"] == "dbr_dev"
    assert (
        trial["variables"]["secret_scope"]
        == lab8["targets"]["prod"]["variables"]["secret_scope"]
    )
    assert trial["resources"]["pipelines"]["wiki_pipeline"]["serverless"] is True
    assert trial["presets"]["trigger_pause_status"] == "PAUSED"
    assert "jobs" not in trial["resources"]
    assert "clusters" not in trial["resources"]["pipelines"]["wiki_pipeline"]
    jobs = {}
    for path in (ROOT / "resources").glob("*.job.yml"):
        jobs.update(yaml.safe_load(path.read_text())["resources"]["jobs"])
    for job in jobs.values():
        assert not {"schedule", "trigger", "continuous"} & job.keys()
        assert job["max_concurrent_runs"] == 1
        for task in job["tasks"]:
            assert (
                "pipeline_task" not in task
            )  # Ingestion is driven exactly once by Python.
            assert (
                ROOT / "resources" / task["notebook_task"]["notebook_path"]
            ).is_file()


def test_ci_targets_prod_and_preserves_lab8_promotion():
    workflow = yaml.safe_load(
        (ROOT.parent / ".github/workflows/lab9-ci.yml").read_text()
    )
    live = workflow["jobs"]["automation"]
    assert live["needs"] == "tests"
    assert live["concurrency"]["cancel-in-progress"] is False
    environment = live["env"]
    assert "azure_trial_2" in environment["LAB9_TARGET"]
    assert "'run'" in environment["LAB9_OPERATION"]
    assert (
        "adb-7405618361895520.0.azuredatabricks.net" in environment["DATABRICKS_HOST"]
    )
    assert PERSONAL_HOST in environment["DATABRICKS_HOST"]
    assert environment["DATABRICKS_AUTH_TYPE"] == "pat"
    # Select the secret NAME first: a missing PROD token must never fall back to DEV.
    assert environment["DATABRICKS_TOKEN"].startswith("${{ secrets[")
    assert "'DATABRICKS_TOKEN'" in environment["DATABRICKS_TOKEN"]
    assert "'DATABRICKS_DEV_TOKEN'" in environment["DATABRICKS_TOKEN"]
    assert "lab8-prod" in live["environment"]
    steps = live["steps"]
    deploy = next(
        step
        for step in steps
        if step.get("name") == "Deploy the selected revision before full orchestration"
    )
    execute = next(
        step
        for step in steps
        if step.get("name") == "Run automation and wait for the result"
    )
    assert deploy["if"] == "env.LAB9_OPERATION == 'run'"
    assert deploy["run"] == 'python -m lab9 --target "$LAB9_TARGET" deploy'
    assert '--target "$LAB9_TARGET"' in execute["run"]
    assert steps.index(deploy) < steps.index(execute)
    historical = yaml.safe_load(
        (ROOT.parent / ".github/workflows/lab8-ci.yml").read_text()
    )
    prod = historical["jobs"]["deploy-prod"]
    assert "github.event_name == 'push'" in prod["if"]
    assert "github.ref == 'refs/heads/main'" in prod["if"]
    prod_commands = [step["run"] for step in prod["steps"] if "run" in step]
    assert any(
        command.startswith("databricks bundle run promotion_job -t prod")
        for command in prod_commands
    )


def test_ci_prod_is_main_only_and_personal_allows_manual_branches():
    workflow = yaml.safe_load(
        (ROOT.parent / ".github/workflows/lab9-ci.yml").read_text()
    )
    triggers = workflow[True]  # YAML 1.1 interprets GitHub's `on` key as True.
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["deployment_target"]["default"] == "azure_trial_2"
    assert inputs["deployment_target"]["options"] == ["azure_trial_2", "personal"]
    assert inputs["operation"]["default"] == "run"
    assert triggers["push"]["branches"] == ["main"]
    live = workflow["jobs"]["automation"]
    assert (
        "inputs.deployment_target == 'personal' || github.ref == 'refs/heads/main'"
        in live["if"]
    )
    assert (
        "github.event_name == 'push' && github.ref == 'refs/heads/main'" in live["if"]
    )


def test_jobs_share_the_lab8_parameter_contract():
    shared = "${var.wikipedia_job_parameters}"
    for path, job in [
        (ROOT.parent / "lab8/resources/promotion.job.yml", "promotion_job"),
        (ROOT / "resources/setup.job.yml", "setup_job"),
        (ROOT / "resources/gold.job.yml", "gold_job"),
    ]:
        assert (
            yaml.safe_load(path.read_text())["resources"]["jobs"][job]["parameters"]
            == shared
        )
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "../lab8/config/*.yml" in bundle["include"]


def test_preflight_names_match_personal_resource_templates():
    for resource, expected in [
        ("setup", JOB_NAMES["setup_job_id"]),
        ("gold", JOB_NAMES["gold_job_id"]),
    ]:
        definition = yaml.safe_load(
            (ROOT / f"resources/{resource}.job.yml").read_text()
        )
        name = definition["resources"]["jobs"][f"{resource}_job"]["name"]
        assert name.replace("${bundle.target}", "personal") == expected
    definition = yaml.safe_load((ROOT / "resources/wikipedia.pipeline.yml").read_text())
    name = definition["resources"]["pipelines"]["wiki_pipeline"]["name"]
    assert name.replace("${bundle.target}", "personal") == PIPELINE_NAME
