"""Explicit negative test: fail a separate pipeline without changing healthy data."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="fail_demo_fact")
@dp.expect_or_fail("one_edit", "edit_count = 1")
def corrupted_fact():
    return (
        spark.read.table(spark.conf.get("lab7.fact_table"))
        .limit(1)
        .withColumn("edit_count", F.lit(-1).cast("long"))
    )
