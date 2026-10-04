"""Transformations only: no writes and no pipeline decorators."""

from pyspark.sql import functions as F

from lab5.contracts import TITLE_COLUMNS, TITLE_SCHEMA, WIKI_SCHEMA


def clean_string(column):
    value = F.trim(F.col(column))
    return F.when(F.length(value) > 0, value)


def read_titles(spark, path):
    # The CSV has quoted newlines, so read whole records.
    return (
        spark.read.schema(TITLE_SCHEMA).format("csv")
        .option("header", "true").option("multiLine", "true")
        .option("quote", '"').option("escape", '"')
        .option("mode", "PERMISSIVE").load(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def clean_titles(df):
    for name in TITLE_COLUMNS:
        df = df.withColumn(name, clean_string(name))
    return (
        df.withColumnRenamed("type", "content_type")
        .withColumnRenamed("listed_in", "genres")
        .withColumnRenamed("duration", "duration_raw")
        .withColumn("release_year", F.expr("try_cast(release_year AS INT)"))
        .withColumn("date_added", F.try_to_timestamp("date_added", F.lit("MMMM d, yyyy")).cast("date"))
    )


def kafka_options(namespace, name, connection_string, max_offsets=1000):
    # Escape quotes and backslashes in the connection string for the JAAS config.
    password = connection_string.replace("\\", "\\\\").replace('"', '\\"')
    return {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "subscribe": name,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            'username="$ConnectionString" ' + f'password="{password}";'
        ),
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
        "maxOffsetsPerTrigger": str(max_offsets),
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "30000",
    }


def decode_kafka(df):
    return df.select(
        F.col("value").cast("string").alias("event_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_enqueued_at"),
    ).withColumn("_ingested_at", F.current_timestamp())


def parse_wiki(df):
    df = df.withColumn("_payload", F.from_json("event_json", WIKI_SCHEMA))
    payload_valid = F.col("_payload").isNotNull() & F.col("_payload._corrupt_record").isNull()
    # Convert only sane unix timestamps; bad ones stay null and go to quarantine.
    unix_time = F.col("_payload.timestamp")
    event_time = F.timestamp_seconds(F.when(unix_time.between(946684800, 4102444800), unix_time))
    fallback_id = F.when(
        F.col("_payload.wiki").isNotNull() & F.col("_payload.id").isNotNull(),
        F.concat_ws(":", F.col("_payload.wiki"), F.col("_payload.id")),
    )
    fields = {
        "event_id": F.coalesce(F.col("_payload.meta.id"), fallback_id),
        "event_type": F.col("_payload.type"),
        "title": F.col("_payload.title"),
        "user_name": F.col("_payload.user"),
        "wiki": F.col("_payload.wiki"),
        "domain": F.col("_payload.meta.domain"),
        "event_time": event_time,
        "namespace": F.col("_payload.namespace"),
        "bot": F.col("_payload.bot"),
        "old_length": F.col("_payload.length.old"),
        "new_length": F.col("_payload.length.new"),
        "old_revision": F.col("_payload.revision.old"),
        "new_revision": F.col("_payload.revision.new"),
        "comment": F.col("_payload.comment"),
        "_payload_valid": payload_valid,
    }
    df = df.select("*", *[value.alias(name) for name, value in fields.items()]).drop("_payload")
    for name in ["event_id", "event_type", "title", "user_name", "wiki", "domain", "comment"]:
        df = df.withColumn(name, clean_string(name))
    return df


def annotate_quality(df, rules):
    errors = F.array(*[
        F.when(~F.coalesce(F.expr(expression), F.lit(False)), F.lit(name))
        for name, expression in rules.items()
    ])
    return (
        df.withColumn("_quality_errors", F.filter(errors, lambda item: item.isNotNull()))
        .withColumn("_is_valid", F.size("_quality_errors") == 0)
    )


def valid_rows(df, rules):
    return annotate_quality(df, rules).filter("_is_valid").drop("_is_valid", "_quality_errors")
