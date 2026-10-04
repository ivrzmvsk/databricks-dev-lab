"""Silver datasets with expectations, plus the quarantine table."""
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("lab5.source_code_path"))
from lab5.contracts import TITLE_RULES, WIKI_RULES
from lab5.transforms import annotate_quality, clean_titles, parse_wiki


@dp.materialized_view(name="titles_silver_lab5", comment="Clean Netflix catalog with expectations")
@dp.expect_all_or_drop(TITLE_RULES)
def titles_silver():
    return clean_titles(spark.read.table("titles_bronze_lab5"))


@dp.temporary_view(name="wiki_prepared_lab5")
def wiki_prepared():
    return annotate_quality(parse_wiki(spark.readStream.table("wiki_bronze_lab5")), WIKI_RULES)


@dp.table(name="wiki_silver_lab5", comment="Wikipedia edits passing all mandatory quality rules")
@dp.expect_all_or_drop(WIKI_RULES)
def wiki_silver():
    # No prefilter: expectations need the bad rows to count the dropped ones.
    return spark.readStream.table("wiki_prepared_lab5").drop("_is_valid", "_quality_errors")


@dp.table(name="wiki_quarantine_lab5", comment="Rejected Wikipedia rows with named quality errors")
def wiki_quarantine():
    return spark.readStream.table("wiki_prepared_lab5").filter("NOT _is_valid")
