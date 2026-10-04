"""Synthetic bad events for the expectations demo. Enabled with quality_demo=true."""
import sys
from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("lab5.source_code_path"))
from lab5.contracts import WIKI_RULES
from lab5.fixtures import wiki_fixture_rows
from lab5.transforms import annotate_quality, parse_wiki

if spark.conf.get("lab5.quality_demo_enabled", "false").lower() == "true":
    @dp.materialized_view(name="wiki_quality_fixture_bronze_lab5")
    def wiki_fixture_bronze():
        return spark.createDataFrame(
            wiki_fixture_rows(),
            "event_json STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_enqueued_at TIMESTAMP, _ingested_at TIMESTAMP",
        )

    @dp.materialized_view(name="wiki_quality_fixture_silver_lab5")
    @dp.expect_all_or_drop(WIKI_RULES)
    def wiki_fixture_silver():
        return parse_wiki(spark.read.table("wiki_quality_fixture_bronze_lab5"))

    @dp.materialized_view(name="wiki_quality_fixture_quarantine_lab5")
    def wiki_fixture_quarantine():
        return annotate_quality(
            parse_wiki(spark.read.table("wiki_quality_fixture_bronze_lab5")), WIKI_RULES
        ).filter("NOT _is_valid")
