"""Run/debug in an IDE using lab7/.venv-connect; Spark runs in personal-env."""

import sys
from pathlib import Path

repo = Path(__file__).resolve().parents[2]
for source in ("lab5/src", "lab6/src", "lab7/src"):
    sys.path.insert(0, str(repo / source))

from databricks.connect import DatabricksSession
from lab7.constraints import BRONZE_SCHEMA
from lab7.quality import classify
from lab7.fixtures import demo_rows


def main():
    spark = DatabricksSession.builder.profile("personal-env").serverless().getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = BRONZE_SCHEMA
    source = spark.createDataFrame(demo_rows(), schema)
    # Set an IDE breakpoint here, or use python -m pdb lab7/tools/debug_connect.py.
    classified = classify(source)
    classified.select("event_id", "_is_valid", "_quality_errors").show(truncate=False)
    print("Remote session:", type(spark).__module__)
    spark.stop()


if __name__ == "__main__":
    main()
