"""Live Wikipedia events; pipeline-owned checkpoints persist across deployments."""
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("lab8.lab5_code_path"))
from lab5.transforms import decode_kafka, kafka_options


@dp.table(
    name="wiki_bronze_lab5",
    comment="Lab 8 Wikipedia raw events with Kafka delivery provenance",
    table_properties={"pipelines.reset.allowed": "false"},
)
def wiki_bronze():
    secret = dbutils.secrets.get(
        scope=spark.conf.get("lab8.secret_scope"),
        key=spark.conf.get("lab8.secret_key"),
    )
    options = kafka_options(
        spark.conf.get("lab8.eventhub_namespace"),
        spark.conf.get("lab8.eventhub_name"),
        secret,
        int(spark.conf.get("lab8.max_offsets_per_trigger")),
    )
    # Spark appends a unique ID; never force kafka.group.id across environments.
    options["groupIdPrefix"] = spark.conf.get("lab8.consumer_group_prefix")
    return decode_kafka(spark.readStream.format("kafka").options(**options).load())
