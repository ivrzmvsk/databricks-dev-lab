"""Retain rejected Wikipedia events and the failed rule names."""
from pyspark import pipelines as dp


@dp.table(name="wiki_quarantine_lab5", comment="Rejected events with named rule failures")
def wiki_quarantine():
    return spark.readStream.table("wiki_prepared_lab5").filter("NOT _is_valid")
