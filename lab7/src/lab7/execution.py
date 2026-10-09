"""Execution boundary: callers choose credentials and inject the DQX engine."""


def build_dqx_engine(spark, workspace_client):
    from databricks.labs.dqx.engine import DQEngineCore
    return DQEngineCore(workspace_client, spark=spark)
