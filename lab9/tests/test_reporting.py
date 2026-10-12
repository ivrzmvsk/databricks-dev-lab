import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from lab9.reporting import Report

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "schemas/report-v3.schema.json").read_text())
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())


@pytest.mark.parametrize("status", ["RUNNING", "SUCCESS", "FAILED", "CANCELED"])
def test_report_lifecycle_matches_versioned_schema(tmp_path, status):
    report = Report(tmp_path / "report.json")
    report.data.update(command="submit-notebook", status=status)
    report.data["stages"] = [
        {
            "kind": "submitted_notebook",
            "run_id": 11,
            "outputs": {
                "probe": {"result": {"sum": 45}, "error": "example", "trace": "details"}
            },
        }
    ]
    if status in {"FAILED", "CANCELED"}:
        report.data.update(
            error="example", cleanup=[{"run_id": 11, "cancellation": "requested"}]
        )
    if status != "RUNNING":
        report.finish()
    report.save(1.23456)
    saved = json.loads(report.path.read_text())
    VALIDATOR.validate(saved)
    assert saved["duration_seconds"] == 1.235
    assert not report.path.with_suffix(".json.tmp").exists()


@pytest.mark.parametrize(
    "field,value", [("compute", "serverless"), ("classic_cluster_created", False)]
)
def test_v3_schema_rejects_unmeasured_compute_claims(tmp_path, field, value):
    report = Report(tmp_path / "report.json")
    report.save(0)
    report.data[field] = value
    assert not VALIDATOR.is_valid(report.data)


@pytest.mark.parametrize("output", ["legacy plain string", {"unexpected": "field"}])
def test_schema_rejects_unstructured_task_output(tmp_path, output):
    report = Report(tmp_path / "report.json")
    report.data["stages"] = [{"kind": "job", "outputs": {"task": output}}]
    report.save(0)
    assert not VALIDATOR.is_valid(report.data)
