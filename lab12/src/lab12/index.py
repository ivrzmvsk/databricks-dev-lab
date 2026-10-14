"""Validation, freshness checks and synchronization of an existing Vector Search index."""

import time

from databricks.sdk.service.vectorsearch import EndpointType, PipelineType, VectorIndexType


def snapshot_matches(response, chunks):
    """Compare every source column, rejecting missing, extra or duplicated rows."""
    expected = {c["chunk_id"]: c for c in chunks}
    rows = []
    for entry in response.get("data", []):
        row = {}
        for field in entry.get("fields", []):
            value = field.get("value", {})
            row[field["key"]] = next(
                (value[k] for k in ("string_value", "number_value", "bool_value") if k in value),
                None,
            )
        rows.append(row)
    if len(rows) != len(expected) or len({r.get("chunk_id") for r in rows}) != len(rows):
        return False
    return all(
        row.get("chunk_id") in expected
        and all(row.get(k) == v for k, v in expected[row["chunk_id"]].items())
        for row in rows
    )


def validate_index(index, settings):
    spec = index.delta_sync_index_spec
    if (
        index.endpoint_name != settings.vector_endpoint
        or index.primary_key != "chunk_id"
        or index.index_type != VectorIndexType.DELTA_SYNC
        or spec is None
        or spec.source_table != settings.table("chunks")
        or spec.pipeline_type != PipelineType.TRIGGERED
    ):
        raise ValueError("Existing index configuration differs from Lab 12")
    embeddings = spec.embedding_source_columns or []
    if (
        len(embeddings) != 1
        or embeddings[0].name != "chunk_text"
        or (embeddings[0].embedding_model_endpoint_name != settings.embedding_model)
    ):
        raise ValueError("Existing embedding configuration differs")


def wait_ready(workspace, settings, expected_rows=None, timeout_seconds=1200):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        index = workspace.vector_search_indexes.get_index(settings.table("chunks_index"))
        if (
            index.status
            and index.status.ready
            and (expected_rows is None or index.status.indexed_row_count == expected_rows)
        ):
            return index
        time.sleep(10)
    raise TimeoutError("Index readiness deadline exceeded")


def sync_index(workspace, settings, expected_rows, timeout_seconds=1200, expected_chunks=None):
    settings.assert_host(workspace.config.host)
    deadline = time.monotonic() + timeout_seconds
    endpoint = workspace.vector_search_endpoints.get_endpoint(settings.vector_endpoint)
    if endpoint.endpoint_type != EndpointType.STANDARD:
        raise ValueError("Existing endpoint is not STANDARD")
    name = settings.table("chunks_index")
    index = workspace.vector_search_indexes.get_index(name)
    validate_index(index, settings)
    # Small-corpus no-op: prove the entire indexed snapshot already matches.
    # Read one extra row to detect stale records; larger corpora use normal sync.
    if (
        expected_chunks
        and len(expected_chunks) < 1000
        and index.status
        and index.status.ready
        and index.status.indexed_row_count == expected_rows
    ):
        scanned = workspace.vector_search_indexes.scan_index(
            name, num_results=len(expected_chunks) + 1
        )
        if snapshot_matches(scanned.as_dict(), expected_chunks):
            return {
                "name": name,
                "rows": expected_rows,
                "ready": True,
                "sync_skipped": True,
            }
    # Confirm the particular update started by this call, rather than trusting
    # the previous index ready flag while a fresh sync is still queued.
    index = workspace.vector_search_indexes.get_index(name)
    # A previous creation may still be provisioning; wait before requesting a new sync.
    while not (index.status and index.status.ready):
        if time.monotonic() >= deadline:
            raise TimeoutError("Existing index provisioning deadline exceeded")
        time.sleep(10)
        index = workspace.vector_search_indexes.get_index(name)
    pipeline_id = index.delta_sync_index_spec.pipeline_id
    if not pipeline_id:
        raise ValueError("Delta Sync index has no pipeline ID")
    previous = workspace.pipelines.list_updates(pipeline_id, max_results=10).updates or []
    previous_ids = {u.update_id for u in previous}
    workspace.vector_search_indexes.sync_index(name)
    update_id = None
    while time.monotonic() < deadline:
        if update_id is None:
            updates = workspace.pipelines.list_updates(pipeline_id, max_results=10).updates or []
            fresh = [u for u in updates if u.update_id not in previous_ids]
            if len(fresh) > 1:
                raise RuntimeError("Concurrent index sync detected; cannot attribute update")
            if not fresh:
                time.sleep(10)
                continue
            update_id = fresh[0].update_id
        state = workspace.pipelines.get_update(pipeline_id, update_id).update.state.value
        if state == "COMPLETED":
            break
        if state in ("FAILED", "CANCELED"):
            raise RuntimeError(f"Index sync update ended in {state}")
        time.sleep(10)
    else:
        raise TimeoutError("Index sync update deadline exceeded")

    wait_ready(workspace, settings, expected_rows, max(0, deadline - time.monotonic()))
    return {"name": name, "rows": expected_rows, "ready": True, "sync_skipped": False}
