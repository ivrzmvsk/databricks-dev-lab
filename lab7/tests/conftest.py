"""Local Spark for CI; no workspace credentials are required."""

import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def dqx_engine(spark):
    """Integration-only dependency; pure transformation tests never create SDK clients."""
    from lab7.execution import build_dqx_engine

    backend = os.environ.get("LAB7_SPARK_BACKEND", "local")
    if backend == "local":
        from unittest.mock import MagicMock

        client = MagicMock()
        client.config._product_info = None
    else:
        from databricks.sdk import WorkspaceClient

        client = (
            WorkspaceClient(profile="personal-env") if backend == "connect" else WorkspaceClient()
        )
    return build_dqx_engine(spark, client)


@pytest.fixture(scope="session")
def spark():
    backend = os.environ.get("LAB7_SPARK_BACKEND", "local")
    if backend == "connect":
        from databricks.connect import DatabricksSession

        session = DatabricksSession.builder.profile("personal-env").serverless().getOrCreate()
        session.conf.set("spark.sql.session.timeZone", "UTC")
        yield session
        session.stop()
        return
    if backend == "runtime":
        import builtins

        session = builtins._lab7_spark
        session.conf.set("spark.sql.session.timeZone", "UTC")
        yield session
        return
    if backend != "local":
        raise ValueError("LAB7_SPARK_BACKEND must be local, connect, or runtime")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[2]")
        .appName("lab7-wikipedia-ci")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
