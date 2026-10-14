"""Provision LAB12 infrastructure outside the data Job (local CLI or GitHub Actions)."""

import argparse
import json
import time
from datetime import timedelta
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

from lab12.config import Settings
from lab12.corpus import prepare
from lab12.index import validate_index, wait_ready
from lab12.storage import quoted, schema_ddl, snapshot_rows

ROOT = Path(__file__).resolve().parents[1]


def execute_sql(workspace, warehouse_id, statement):
    result = workspace.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="50s"
    )
    deadline = time.monotonic() + 1200
    while result.status.state.value in ("PENDING", "RUNNING"):
        if time.monotonic() >= deadline:
            workspace.statement_execution.cancel_execution(result.statement_id)
            raise TimeoutError("Infrastructure SQL deadline exceeded")
        time.sleep(5)
        result = workspace.statement_execution.get_statement(result.statement_id)
    if result.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"Infrastructure SQL failed: {result.status.error}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("dev", "prod"), required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--warehouse-id", required=True)
    args = parser.parse_args()
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    variables = {k: v["default"] for k, v in bundle["variables"].items() if "default" in v}
    variables.update(bundle["targets"][args.target].get("variables", {}))
    settings = Settings(target=args.target, **variables)
    workspace = WorkspaceClient(profile=args.profile)
    settings.assert_host(workspace.config.host)

    # Create schema and empty CDF tables. No document rows are loaded here.
    documents, chunks = prepare(ROOT / "dataset/board_games.json")
    execute_sql(
        workspace,
        args.warehouse_id,
        f"CREATE SCHEMA IF NOT EXISTS {quoted(settings.catalog + '.' + settings.schema)}",
    )
    for table, rows, _ in snapshot_rows(documents, chunks):
        execute_sql(
            workspace,
            args.warehouse_id,
            f"CREATE TABLE IF NOT EXISTS {quoted(settings.table(table))} "
            f"({schema_ddl(rows)}) USING DELTA "
            "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
        )

    # Provision endpoint/index only when absent; incompatible resources are preserved.
    try:
        endpoint = workspace.vector_search_endpoints.get_endpoint(settings.vector_endpoint)
        if endpoint.endpoint_type != EndpointType.STANDARD:
            raise ValueError("Existing endpoint is not STANDARD")
    except NotFound:
        workspace.vector_search_endpoints.create_endpoint(
            settings.vector_endpoint, EndpointType.STANDARD
        ).result(timeout=timedelta(seconds=1200))
    created = False
    name = settings.table("chunks_index")
    try:
        validate_index(workspace.vector_search_indexes.get_index(name), settings)
    except NotFound:
        workspace.vector_search_indexes.create_index(
            name,
            settings.vector_endpoint,
            "chunk_id",
            VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=settings.table("chunks"),
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name="chunk_text", embedding_model_endpoint_name=settings.embedding_model
                    )
                ],
            ),
        )
        created = True
    index = wait_ready(workspace, settings)
    print(
        json.dumps(
            {
                "target": settings.target,
                "host": workspace.config.host,
                "schema": f"{settings.catalog}.{settings.schema}",
                "endpoint": settings.vector_endpoint,
                "index": name,
                "index_created": created,
                "ready": index.status.ready,
                "indexed_rows": index.status.indexed_row_count,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
