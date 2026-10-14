from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.vectorsearch import EndpointType, PipelineType, VectorIndexType

from lab12.config import DEV_HOST, Settings
from lab12.index import sync_index
from lab12.qa import ServingModel


def workspace():
    client = Mock()
    client.config.host = DEV_HOST
    return client


def test_permission_error_does_not_create_an_endpoint():
    client = workspace()
    client.vector_search_endpoints.get_endpoint.side_effect = PermissionDenied("denied")
    with pytest.raises(PermissionDenied):
        sync_index(client, Settings(), 12)
    client.vector_search_endpoints.create_endpoint.assert_not_called()
    client.vector_search_indexes.create_index.assert_not_called()


def test_incompatible_index_is_not_replaced():
    client = workspace()
    client.vector_search_endpoints.get_endpoint.return_value = SimpleNamespace(
        endpoint_type=EndpointType.STANDARD
    )
    client.vector_search_indexes.get_index.return_value = SimpleNamespace(
        endpoint_name=Settings().vector_endpoint,
        primary_key="chunk_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=SimpleNamespace(
            source_table="workspace.other.chunks", pipeline_type=PipelineType.TRIGGERED
        ),
    )
    with pytest.raises(ValueError, match="configuration differs"):
        sync_index(client, Settings(), 12)
    client.vector_search_indexes.create_index.assert_not_called()
    client.vector_search_indexes.sync_index.assert_not_called()


@pytest.mark.parametrize(
    "response",
    [
        {"choices": []},
        {"choices": [{"message": {"content": ""}, "finish_reason": "stop"}]},
        {"choices": [{"message": {"content": "Partial answer"}, "finish_reason": "length"}]},
        {"choices": [{"message": {"content": "Answer without completion status"}}]},
    ],
)
def test_failed_model_output_is_not_treated_as_refusal(response):
    client = workspace()
    client.api_client.do.return_value = response
    with pytest.raises(ValueError):
        ServingModel(client, Settings()).complete([{"role": "user", "content": "Ocean games?"}])


def test_serving_diagnostics_are_preserved():
    client = workspace()
    client.api_client.do.return_value = {
        "choices": [{"message": {"content": "An ocean game [1]."}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 10},
    }
    assert ServingModel(client, Settings()).complete(
        [{"role": "user", "content": "Ocean games?"}]
    ) == ("An ocean game [1].", "stop", 100, 10)


def test_retry_waits_for_provisioning_before_sync(monkeypatch):
    import lab12.index as index_module

    client = workspace()
    settings = Settings()
    client.vector_search_endpoints.get_endpoint.return_value = SimpleNamespace(
        endpoint_type=EndpointType.STANDARD
    )
    spec = SimpleNamespace(
        source_table=settings.table("chunks"),
        pipeline_type=PipelineType.TRIGGERED,
        pipeline_id="pipeline",
        embedding_source_columns=[
            SimpleNamespace(
                name="chunk_text", embedding_model_endpoint_name=settings.embedding_model
            )
        ],
    )

    def state(ready):
        return SimpleNamespace(
            endpoint_name=settings.vector_endpoint,
            primary_key="chunk_id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=spec,
            status=SimpleNamespace(ready=ready, indexed_row_count=6 if ready else 0),
        )

    pending, ready = state(False), state(True)
    client.vector_search_indexes.get_index.side_effect = [pending, pending, ready, ready]
    client.pipelines.list_updates.side_effect = [
        SimpleNamespace(updates=[]),
        SimpleNamespace(updates=[SimpleNamespace(update_id="new")]),
    ]
    client.pipelines.get_update.return_value = SimpleNamespace(
        update=SimpleNamespace(state=SimpleNamespace(value="COMPLETED"))
    )
    sleeps = []

    def wait(seconds):
        client.vector_search_indexes.sync_index.assert_not_called()
        sleeps.append(seconds)

    monkeypatch.setattr(index_module.time, "sleep", wait)
    result = sync_index(client, settings, 6)
    assert result["ready"] and sleeps == [10]
    client.vector_search_indexes.sync_index.assert_called_once()


def test_index_snapshot_requires_all_rows_and_metadata():
    from lab12.index import snapshot_matches

    chunks = [{"chunk_id": "a", "chunk_text": "Rules", "coop": True}]
    response = {
        "data": [
            {
                "fields": [
                    {"key": "chunk_id", "value": {"string_value": "a"}},
                    {"key": "chunk_text", "value": {"string_value": "Rules"}},
                    {"key": "coop", "value": {"bool_value": True}},
                ]
            }
        ]
    }
    assert snapshot_matches(response, chunks)
    assert not snapshot_matches({"data": response["data"] * 2}, chunks)
    assert not snapshot_matches({"data": []}, chunks)
    response["data"][0]["fields"][-1]["value"]["bool_value"] = False
    assert not snapshot_matches(response, chunks)


def test_sync_does_not_provision_a_missing_index():
    from databricks.sdk.errors import NotFound

    client = workspace()
    client.vector_search_endpoints.get_endpoint.return_value = SimpleNamespace(
        endpoint_type=EndpointType.STANDARD
    )
    client.vector_search_indexes.get_index.side_effect = NotFound("Run setup first")
    with pytest.raises(NotFound):
        sync_index(client, Settings(), 12)
    client.vector_search_indexes.create_index.assert_not_called()
    client.vector_search_endpoints.create_endpoint.assert_not_called()
