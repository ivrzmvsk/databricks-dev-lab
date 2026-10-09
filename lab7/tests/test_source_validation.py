import pytest
from pyspark.sql import functions as F

from lab7.source_validation import DIMENSION_KEYS, AGGREGATE_TABLES, validate_gold_frames


@pytest.fixture
def source_frames(spark):
    fact = spark.createDataFrame(
        [("a", "w", 1, "n", "p", "e", 1, 10, 10, 0)],
        "event_id STRING, wiki_key STRING, date_key INT, namespace_key STRING, "
        "page_key STRING, editor_key STRING, edit_count LONG, bytes_delta LONG, "
        "bytes_added LONG, bytes_removed LONG",
    )
    silver = fact.select("event_id")
    dimensions = {name: fact.select(key).distinct() for name, key in DIMENSION_KEYS.items()}
    total = spark.createDataFrame([(1, 10)], "edit_count LONG, net_bytes_delta LONG")
    return fact, silver, dimensions, {name: total for name in AGGREGATE_TABLES}


def test_source_gold_reconciliation_accepts_healthy_frames(source_frames):
    assert validate_gold_frames(*source_frames) == 1


@pytest.mark.parametrize(
    "corruption,reason",
    [
        ("duplicate_fact", "Duplicate events"),
        ("missing_silver", "pinned Silver snapshot"),
        ("orphan_page", "Orphan page_key"),
        ("aggregate_count", "agg_wiki_daily count mismatch"),
        ("aggregate_bytes", "agg_wiki_daily bytes mismatch"),
        ("fact_bytes", "Inconsistent fact byte measures"),
    ],
)
def test_source_gold_reconciliation_rejects_corrupt_data(source_frames, corruption, reason):
    fact, silver, dimensions, aggregates = source_frames
    if corruption == "duplicate_fact":
        fact = fact.unionByName(fact)
    elif corruption == "missing_silver":
        silver = silver.limit(0)
    elif corruption == "orphan_page":
        dimensions["dim_page"] = dimensions["dim_page"].limit(0)
    elif corruption == "aggregate_count":
        aggregates["agg_wiki_daily"] = aggregates["agg_wiki_daily"].withColumn(
            "edit_count", F.lit(2)
        )
    elif corruption == "aggregate_bytes":
        aggregates["agg_wiki_daily"] = aggregates["agg_wiki_daily"].withColumn(
            "net_bytes_delta", F.lit(11)
        )
    elif corruption == "fact_bytes":
        fact = fact.withColumn("bytes_added", F.lit(9))
    with pytest.raises(AssertionError, match=reason):
        validate_gold_frames(fact, silver, dimensions, aggregates)
