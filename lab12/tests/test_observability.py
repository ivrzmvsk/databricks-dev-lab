"""Regression checks for UC trace consistency and fail-closed readback."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lab12.observability import verify_trace


def mlflow_stub(monkeypatch):
    stub = MagicMock()
    monkeypatch.setitem(sys.modules, "mlflow", stub)
    return stub


def test_captured_trace_is_verified_without_search(monkeypatch):
    stub = mlflow_stub(monkeypatch)
    trace_id = "trace:/workspace.lab12.prefix/current"
    stub.MlflowClient.return_value.get_trace.return_value = SimpleNamespace(
        info=SimpleNamespace(trace_id=trace_id, request_time=1100),
        data=SimpleNamespace(
            spans=[SimpleNamespace(span_type=k) for k in ("CHAIN", "RETRIEVER", "LLM")]
        ),
    )
    result = verify_trace("123", "https://dev.example", 1000, trace_id)
    assert result["spans"] == 3
    assert result["url"] == "https://dev.example/ml/experiments/123/traces/current"
    stub.MlflowClient.return_value.get_trace.assert_called_once_with(trace_id, display=False)
    stub.search_traces.assert_not_called()


def test_failed_trace_read_does_not_fall_back(monkeypatch):
    stub = mlflow_stub(monkeypatch)
    stub.MlflowClient.return_value.get_trace.side_effect = RuntimeError("RESOURCE_EXHAUSTED")
    with pytest.raises(RuntimeError, match="RESOURCE_EXHAUSTED"):
        verify_trace("123", "https://dev.example", 1000, "trace:/workspace.lab12.prefix/current")
    stub.search_traces.assert_not_called()
