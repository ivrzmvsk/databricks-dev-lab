"""Explicit Unity Catalog tracing setup and readback; never silently downgrade storage."""

import os
import time


def configure(workspace, settings, experiment_name, warehouse_id, profile=None):
    import mlflow
    from mlflow.entities.trace_location import UnityCatalog

    settings.assert_host(workspace.config.host)
    if not warehouse_id or not experiment_name.startswith("/"):
        raise ValueError("Warehouse ID and absolute experiment path are required")
    if profile:
        # MLflow's warehouse helper creates WorkspaceClient() independently of
        # the tracking URI; bind it to the same explicit DEV/PROD identity.
        os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
        os.environ["DATABRICKS_HOST"] = settings.host
    mlflow.set_tracking_uri(f"databricks://{profile}" if profile else "databricks")
    os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id
    experiment = mlflow.set_experiment(
        experiment_name=experiment_name,
        trace_location=UnityCatalog(catalog_name=settings.catalog, schema_name=settings.schema),
    )
    if not isinstance(experiment.trace_location, UnityCatalog):
        raise RuntimeError("Experiment is not configured for Unity Catalog tracing")
    if (experiment.trace_location.catalog_name, experiment.trace_location.schema_name) != (
        settings.catalog,
        settings.schema,
    ):
        raise RuntimeError("Experiment trace location differs from configured schema")
    return experiment.experiment_id


def verify_trace(experiment_id, host, started_ms, trace_id=None):
    import mlflow

    mlflow.flush_trace_async_logging()
    if trace_id:
        # UC trace ingestion is eventually consistent. The native client retries
        # only "not found"; auth, quota and warehouse errors still fail immediately.
        os.environ["MLFLOW_GET_TRACE_OTEL_RETRY_TIMEOUT_SECONDS"] = "120"
        trace = mlflow.MlflowClient().get_trace(trace_id, display=False)
    else:
        traces = mlflow.search_traces(
            locations=[experiment_id],
            filter_string=f"trace.timestamp_ms >= {started_ms}",
            max_results=1,
            return_type="list",
            include_spans=True,
        )
        if len(traces) != 1:
            raise RuntimeError("No trace from the current run was found")
        trace = traces[0]
    if trace.info.request_time < started_ms:
        raise RuntimeError("Trace predates the current verification run")
    trace_id = trace.info.trace_id
    kinds = {s.span_type for s in trace.data.spans}
    if not {"CHAIN", "RETRIEVER", "LLM"}.issubset(kinds):
        raise RuntimeError(f"Expected QA, retrieval and LLM spans; found {kinds}")
    return {
        "trace_id": trace_id,
        "spans": len(trace.data.spans),
        "url": f"{host}/ml/experiments/{experiment_id}/traces/{trace_id.rsplit('/', 1)[-1]}",
        "verified_at_ms": int(time.time() * 1000),
    }
