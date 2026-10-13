"""Failure-path behavior: correct error propagation and conflict gating."""

from unittest.mock import Mock

import pytest

from lab11 import common, processing, producer


@pytest.mark.parametrize("stage", ["enqueue", "flush", "close"])
def test_send_failure_never_reports_success(monkeypatch, stage):
    stream = Mock()
    original = RuntimeError(stage + " failed")
    method = {"enqueue": "ingest_record_offset", "flush": "flush", "close": "close"}[stage]
    getattr(stream, method).side_effect = original
    if stage != "close":
        stream.close.side_effect = RuntimeError("cleanup failed")
    sdk = Mock()
    sdk.create_stream.return_value = stream
    monkeypatch.setattr(producer, "ZerobusSdk", lambda *args: sdk)
    monkeypatch.setattr(producer, "credentials", lambda target="personal": ("test-id", "test-secret"))
    with pytest.raises(RuntimeError) as error:
        producer.send([{"event_id": "source-id"}], "run-id")
    assert error.value is original


def test_conflict_prevents_merge():
    execute = Mock(return_value=[["conflicting-source-id"]])
    with pytest.raises(ValueError, match="Conflicting business payload"):
        processing.process(execute)
    assert execute.call_count == 1
    assert "MERGE" not in execute.call_args.args[0]


def test_sql_timeout_cancels_statement(monkeypatch):
    client = Mock()
    response = client.statement_execution.execute_statement.return_value
    response.status.state.value = "RUNNING"
    response.statement_id = "timeout-statement"
    monkeypatch.setattr(common.time, "monotonic", Mock(side_effect=[0, 301]))
    with pytest.raises(TimeoutError):
        common.sql(client, "SELECT 1")
    client.statement_execution.cancel_execution.assert_called_once_with("timeout-statement")


def test_source_disconnect_aborts_capture():
    with pytest.raises(ConnectionError, match="interrupted"):
        producer.source_error()
