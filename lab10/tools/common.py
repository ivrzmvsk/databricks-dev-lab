"""Explicit, host-pinned workspace clients. Never print connection credentials."""

import time
from pathlib import Path

from databricks.sdk import WorkspaceClient

ROOT = Path(__file__).resolve().parents[1]
TARGETS = {
    "personal": ("personal-env", "https://dbc-ab9a5afe-c151.cloud.databricks.com"),
    "azure_trial_2": ("azure_trial_2", "https://adb-7405618361895520.0.azuredatabricks.net"),
}


def workspace_client(target="personal"):
    profile, host = TARGETS[target]
    client = WorkspaceClient(profile=profile)
    if client.config.host.rstrip("/") != host:
        raise RuntimeError(f"{profile} must resolve to its pinned workspace")
    return client


def dev_client():
    return workspace_client("personal")


class WarehouseSQL:
    def __init__(self, client, warehouse_id):
        self.client = client
        self.warehouse_id = warehouse_id

    def __call__(self, statement):
        api = self.client.statement_execution
        result = api.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=statement,
            wait_timeout="10s",
            row_limit=10000,
        )
        deadline = time.monotonic() + 600
        while result.status.state.value in ("PENDING", "RUNNING"):
            if time.monotonic() >= deadline:
                api.cancel_execution(result.statement_id)
                raise TimeoutError("SQL statement exceeded 600 seconds")
            time.sleep(2)
            result = api.get_statement(result.statement_id)
        if result.status.state.value != "SUCCEEDED":
            error = result.status.error
            raise RuntimeError(f"SQL failed: {error.error_code}: {error.message}")
        if not result.manifest or not result.manifest.schema:
            return []
        if result.manifest.truncated:
            raise RuntimeError("SQL result truncated; narrow the query")
        columns = [c.name for c in result.manifest.schema.columns]
        rows = list(result.result.data_array or []) if result.result else []
        chunk = result.result
        while chunk and chunk.next_chunk_index is not None:
            chunk = api.get_statement_result_chunk_n(result.statement_id, chunk.next_chunk_index)
            rows.extend(chunk.data_array or [])
        return [dict(zip(columns, row)) for row in rows]


def sql_client(target="personal"):
    client = workspace_client(target)
    warehouses = list(client.warehouses.list())
    servers = [w for w in warehouses if w.enable_serverless_compute]
    if len(servers) != 1:
        raise RuntimeError("Expected exactly one serverless SQL warehouse in the selected workspace")
    return WarehouseSQL(client, servers[0].id)
