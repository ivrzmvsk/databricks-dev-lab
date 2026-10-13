import importlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
import yaml
from cleanup_negative_dev import table_names

from lab11 import common, producer

ROOT = Path(__file__).resolve().parents[1]


def test_sql_split_preserves_quoted_and_comment_semicolons():
    sql = "-- comment;\nCREATE TABLE a (x STRING COMMENT 'a;b'); /* block; */ SELECT 'c;d';"
    chunks = common.statements(sql)
    assert len(chunks) == 2
    assert "'a;b'" in chunks[0] and "'c;d'" in chunks[1]
    assert common.statements("-- only a comment;") == []


@pytest.mark.parametrize("module", ["check_access", "setup_dev"])
def test_imports_do_not_access_workspace(monkeypatch, module):
    client = Mock(side_effect=AssertionError("Network on import"))
    monkeypatch.setattr(common, "dev_client", client)
    importlib.reload(importlib.import_module(module))
    client.assert_not_called()


@pytest.mark.parametrize("state", ["CANCELED", "CLOSED", "FAILED"])
def test_terminal_sql_error_without_details(state):
    client = Mock()
    client.statement_execution.execute_statement.return_value = NS(
        status=NS(state=NS(value=state), error=None), statement_id="sql-id"
    )
    with pytest.raises(RuntimeError, match=state + ": No error details"):
        common.sql(client, "SELECT 1")


def test_sql_reads_multiple_chunks_without_row_limit():
    client = Mock()
    client.statement_execution.execute_statement.return_value = NS(
        status=NS(state=NS(value="SUCCEEDED")),
        statement_id="sql-id",
        manifest=NS(truncated=False),
        result=NS(data_array=[[x] for x in range(1000)], next_chunk_index=1),
    )
    client.statement_execution.get_statement_result_chunk_n.return_value = NS(
        data_array=[[1000]], next_chunk_index=None
    )
    assert len(common.sql(client, "SELECT many_rows")) == 1001
    assert "row_limit" not in client.statement_execution.execute_statement.call_args.kwargs


def test_invalid_duplicates_does_not_capture_or_create_file(monkeypatch, tmp_path):
    manifest = tmp_path / "must-not-exist.json"
    monkeypatch.setattr(
        sys, "argv", ["producer", "--count", "1", "--duplicates", "2", "--manifest", str(manifest)]
    )
    capture = Mock(side_effect=AssertionError("Capture should not happen"))
    monkeypatch.setattr(producer, "capture", capture)
    with pytest.raises(SystemExit) as error:
        producer.main()
    assert error.value.code == 2
    capture.assert_not_called()
    assert not manifest.exists()


def test_default_fixture_does_not_depend_on_state(monkeypatch, tmp_path):
    monkeypatch.setattr(common, "ROOT", tmp_path)
    manifest = common.load_manifest()
    assert len(manifest["records"]) == 3
    assert not (tmp_path / ".state").exists()


def test_validation_import_does_not_load_ingest_sdk():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import validate_dev; assert 'zerobus' not in sys.modules; "
            "assert 'requests_sse' not in sys.modules",
        ],
        capture_output=True,
        text=True,
        cwd=ROOT / "tools",
    )
    assert result.returncode == 0, result.stderr


def test_template_parameters_are_quoted_and_validated():
    sql = common.render_sql("01_merge.sql", bronze="catalog.schema.bronze", silver="catalog.schema.silver")
    assert "`catalog`.`schema`.`bronze`" in sql
    assert "${" not in sql
    with pytest.raises(ValueError):
        common.render_sql("01_merge.sql", bronze="a.b.c; DROP SCHEMA a")


@pytest.mark.parametrize("suffix", ["main", "../x", "abc;drop", ""])
def test_cleanup_cannot_target_main_tables(suffix):
    with pytest.raises(ValueError):
        table_names(suffix)


def test_bundle_targets_and_no_automatic_prod_workflow():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["targets"]["personal"]["default"] is True
    assert bundle["targets"]["azure_trial_2"]["mode"] == "production"
    assert bundle["targets"]["personal"]["workspace"]["host"] == common.HOST
    assert bundle["targets"]["azure_trial_2"]["workspace"]["host"] == common.TARGETS["azure_trial_2"]["host"]
    workflow = (ROOT.parent / ".github/workflows/lab11-ci.yml").read_text()
    assert "bundle deploy" not in workflow


def test_capture_reports_malformed_and_filtered_counts(monkeypatch, caplog):
    caplog.set_level("INFO", logger="lab11.producer")
    events = [
        NS(type="message", data="[]"),
        NS(type="message", data='{"type":"new"}'),
        NS(
            type="message",
            data=json.dumps(
                {
                    "meta": {"id": "valid"},
                    "wiki": "enwiki",
                    "type": "edit",
                    "title": "Example",
                    "user": "Editor",
                    "length": {"old": 1, "new": 2},
                    "bot": False,
                    "id": 1,
                    "timestamp": 1,
                }
            ),
        ),
    ]
    context = Mock()
    context.__enter__ = Mock(return_value=iter(events))
    context.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(producer, "EventSource", lambda *args, **kwargs: context)
    assert len(producer.capture(1, 10)) == 1
    assert "Skipping malformed" in caplog.text
    assert "'malformed': 1" in caplog.text and "'filtered': 1" in caplog.text


def test_producer_uses_selected_environment_without_network(monkeypatch):
    sdk = Mock()
    constructor = Mock(return_value=sdk)
    credentials = Mock(return_value=("test-id", "test-secret"))
    monkeypatch.setattr(producer, "ZerobusSdk", constructor)
    monkeypatch.setattr(producer, "credentials", credentials)
    producer.send([{"event_id": "test"}], "test-run", target="azure_trial_2")
    config = common.TARGETS["azure_trial_2"]
    constructor.assert_called_once_with(
        config.get("ingest_endpoint", config["endpoint"]), config.get("ingest_host", config["host"])
    )
    credentials.assert_called_once_with("azure_trial_2")
    with pytest.raises(ValueError, match="selected target schema"):
        producer.send([], "test", target="azure_trial_2", table_name=common.BRONZE)


def test_tool_entry_points_are_import_safe():
    import runpy

    for path in (ROOT / "tools").glob("*.py"):
        runpy.run_path(str(path), run_name="import_check")


def test_native_trial2_ingest_and_compute_are_aligned():
    config = common.TARGETS["azure_trial_2"]
    assert "7405618361895520" in config["host"]
    assert config["ingest_host"] == config["host"]
    assert config["ingest_endpoint"] == config["endpoint"]
    assert "7405618361895520.zerobus" in config["ingest_endpoint"]
    assert config["schema"] == "dbr_dev.ivanrazumovskyi_lab11_zerobus"
