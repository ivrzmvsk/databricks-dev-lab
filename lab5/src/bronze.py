"""Bronze datasets. Runs inside the Lakeflow pipeline."""
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("lab5.source_code_path"))
from lab5.transforms import decode_kafka, kafka_options, read_titles


@dp.materialized_view(name="titles_bronze_lab5", comment="Static Netflix CSV, batch ingestion")
def titles_bronze():
    return read_titles(spark, spark.conf.get("lab5.source_csv_path"))


@dp.table(
    name="wiki_bronze_lab5",
    comment="Raw Wikipedia JSON and Kafka offsets; retained for downstream replay",
    table_properties={"pipelines.reset.allowed": "false"},
)
def wiki_bronze():
    secret = dbutils.secrets.get(
        scope=spark.conf.get("lab5.secret_scope"), key=spark.conf.get("lab5.secret_key")
    )
    options = kafka_options(
        spark.conf.get("lab5.eventhub_namespace"),
        spark.conf.get("lab5.eventhub_name"), secret,
        int(spark.conf.get("lab5.max_offsets_per_trigger", "1000")),
    )
    return decode_kafka(spark.readStream.format("kafka").options(**options).load())
