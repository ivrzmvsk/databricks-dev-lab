import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (SparkSession.builder.master("local[2]")
               .appName("lab8-wikipedia-ci")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.ansi.enabled", "true").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
