"""Lab 5 parsing and expectations, restricted to Wikipedia."""
import sys

from pyspark import pipelines as dp

for path in ("lab8.lab5_code_path", "lab8.lab8_code_path"):
    sys.path.insert(0, spark.conf.get(path))
from lab5.transforms import annotate_quality, parse_wiki
from lab8.contracts import RULES


@dp.temporary_view(name="wiki_prepared_lab5")
def wiki_prepared():
    return annotate_quality(parse_wiki(spark.readStream.table("wiki_bronze_lab5")), RULES)


@dp.table(name="wiki_silver_lab5", comment="Wikipedia edits satisfying the Gold contract")
@dp.expect_all_or_drop(RULES)
def wiki_silver():
    return spark.readStream.table("wiki_prepared_lab5").drop("_is_valid", "_quality_errors")
