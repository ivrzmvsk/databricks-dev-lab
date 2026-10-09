"""Wikipedia-only batch pipeline over a frozen Lab 5 Bronze snapshot."""
import sys
from pyspark import pipelines as dp
from pyspark.sql import functions as F

root = spark.conf.get("lab7.code_root")
for path in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, f"{root}/{path}")
from lab7.quality import ROW_RULES, FACT_RULES, classify, rank_deliveries, make_fact, make_dimensions, make_aggregates, enrich_wikis


@dp.materialized_view(name="wiki_bronze")
def bronze():
    return spark.read.table(spark.conf.get("lab7.input_table"))


@dp.temporary_view(name="wiki_classified")
def classified():
    return classify(spark.read.table("wiki_bronze"))


@dp.materialized_view(name="wiki_accepted")
@dp.expect_all_or_drop(ROW_RULES)
def accepted():
    return spark.read.table("wiki_classified")


@dp.materialized_view(name="wiki_quarantine")
def quarantine():
    return spark.read.table("wiki_classified").filter("NOT _is_valid")


@dp.temporary_view(name="wiki_ranked")
def ranked():
    return rank_deliveries(spark.read.table("wiki_accepted"))


@dp.materialized_view(name="wiki_silver")
def silver():
    return spark.read.table("wiki_ranked").filter("_delivery = 1").drop("_delivery")


@dp.materialized_view(name="wiki_duplicates")
def duplicates():
    return spark.read.table("wiki_ranked").filter("_delivery > 1").withColumn("_duplicate_reason", F.lit("repeated_event_id"))


@dp.materialized_view(name="fact_candidate")
@dp.expect_all_or_fail(FACT_RULES)
def fact():
    return make_fact(spark.read.table("wiki_silver"))


def register_dimension(name):
    @dp.materialized_view(name=name)
    def dataset():
        data = make_dimensions(spark.read.table("fact_candidate"))[name]
        if name == "dim_wiki":
            return enrich_wikis(data, spark.read.table(spark.conf.get("lab7.reference_table")))
        return data


for name in ("dim_wiki", "dim_page", "dim_editor", "dim_namespace", "dim_date"):
    register_dimension(name)


def register_aggregate(name):
    @dp.materialized_view(name=name)
    def dataset():
        return make_aggregates(spark.read.table("fact_candidate"))[name]


for name in ("agg_wiki_daily", "agg_wiki_5min"):
    register_aggregate(name)
