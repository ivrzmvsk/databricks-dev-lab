import json

from pyspark.sql import functions as F

from lab5.fixtures import wiki_fixture_rows
from lab5.transforms import annotate_quality, parse_wiki
from lab6.transforms import aggregate_activity, prepare_edits
from lab8.contracts import RULES


def source(spark):
    rows = wiki_fixture_rows()
    # Same event replayed at another Kafka offset, one new edit, one missing key.
    event = json.loads(rows[0][0])
    event["meta"]["id"] = "other-edit"
    event["length"] = {"old": 120, "new": 100}
    other = (json.dumps(event), *rows[0][1:3], 8, *rows[0][4:])
    event["meta"]["id"] = "missing-namespace"
    event["namespace"] = None
    missing = (json.dumps(event), *rows[0][1:3], 9, *rows[0][4:])
    replay = (rows[0][0], *rows[0][1:3], 7, *rows[0][4:])
    return spark.createDataFrame(
        rows + [replay, other, missing],
        "event_json STRING, kafka_topic STRING, kafka_partition INT, kafka_offset LONG, "
        "kafka_enqueued_at TIMESTAMP, _ingested_at TIMESTAMP",
    )


def test_quality_partition_and_gold_replay(spark):
    raw = source(spark)
    annotated = annotate_quality(parse_wiki(raw), RULES)
    good = annotated.filter("_is_valid").drop("_is_valid", "_quality_errors")
    bad = annotated.filter("NOT _is_valid")
    assert good.count() == 3
    assert bad.count() == 7
    assert good.count() + bad.count() == raw.count()
    invalid_key = bad.filter("event_id = 'missing-namespace'").first()
    assert invalid_key._quality_errors == ["valid_namespace"]
    edits = prepare_edits(good)
    assert edits.count() == 2
    aggregate = aggregate_activity(edits, ["wiki"]).first()
    assert aggregate.edit_count == 2
    assert aggregate.net_bytes_delta == -10
    assert aggregate.editor_count == 1
    # Replaying every delivery again changes neither business rows nor totals.
    replayed = prepare_edits(good.unionByName(good))
    assert edits.exceptAll(replayed).count() == 0
    assert replayed.exceptAll(edits).count() == 0


def test_unknown_lengths_remain_unknown(spark):
    good = annotate_quality(parse_wiki(source(spark)), RULES).filter("_is_valid")
    edits = prepare_edits(good.withColumn("old_length", F.lit(None).cast("long")))
    assert (
        edits.filter(
            "bytes_delta IS NOT NULL OR bytes_added IS NOT NULL OR bytes_removed IS NOT NULL"
        ).count()
        == 0
    )
