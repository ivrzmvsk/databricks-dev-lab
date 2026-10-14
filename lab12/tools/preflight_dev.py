"""Read-only DEV inventory. Does not start compute, run SQL or invoke models."""

import json
from pathlib import Path

from databricks.sdk import WorkspaceClient

from lab12.config import Settings


def main():
    settings = Settings()
    workspace = WorkspaceClient(profile="personal-env")
    settings.assert_host(workspace.config.host)
    user = workspace.current_user.me()
    endpoints = {e.name: e for e in workspace.serving_endpoints.list()}
    for name in (settings.embedding_model, settings.chat_model):
        if name not in endpoints or endpoints[name].state.ready.value != "READY":
            raise RuntimeError(f"Required model not ready: {name}")
    warehouses = sorted(
        workspace.warehouses.list(),
        key=lambda w: (
            w.state.value != "RUNNING",
            w.id,
        ),
    )
    if not warehouses:
        raise RuntimeError("No SQL warehouse available for UC traces")
    report = {
        "host": workspace.config.host,
        "profile": "personal-env",
        "user": user.user_name,
        "warehouse_id": warehouses[0].id,
        "warehouse_state": warehouses[0].state.value,
        "embedding_model": settings.embedding_model,
        "chat_model": settings.chat_model,
        "vector_endpoints": [e.name for e in workspace.vector_search_endpoints.list_endpoints()],
        "schema_candidate": f"{settings.catalog}.{settings.schema}",
        "limitations": [
            "Readiness is not proof of CAN_QUERY or inference success",
            "Schema creation and tracing are not tested by this inventory",
        ],
    }
    output = Path(__file__).resolve().parents[1] / ".state/preflight.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
