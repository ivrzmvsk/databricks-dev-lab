"""Pure transformations, shared by notebooks and local Spark tests."""
from pyspark.sql import Window, functions as F


def scoped_key(*columns):
    # Struct JSON preserves boundaries and nulls; wiki-scoped names never collide.
    return F.sha2(F.to_json(F.struct(*[F.col(c).alias(c) for c in columns])), 256)


def prepare_edits(source):
    order = Window.partitionBy("event_id").orderBy(
        "kafka_enqueued_at", "kafka_topic", "kafka_partition", "kafka_offset"
    )
    return (
        source.withColumn("_delivery", F.row_number().over(order))
        .filter("_delivery = 1")
        .select(
            "event_id", "event_time", "wiki", "title", "user_name", "namespace",
            "bot", "old_length", "new_length", "old_revision", "new_revision",
        )
        .withColumn("wiki_key", scoped_key("wiki"))
        .withColumn("page_key", scoped_key("wiki", "title"))
        .withColumn("editor_key", scoped_key("wiki", "user_name"))
        .withColumn("namespace_key", scoped_key("wiki", "namespace"))
        .withColumn("date_key", F.date_format("event_time", "yyyyMMdd").cast("int"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("bucket_start", F.window("event_time", "5 minutes").getField("start"))
        .withColumn("edit_count", F.lit(1).cast("long"))
        .withColumn("bytes_delta", F.col("new_length") - F.col("old_length"))
        .withColumn("bytes_added", F.greatest("bytes_delta", F.lit(0)))
        .withColumn("bytes_removed", F.greatest(-F.col("bytes_delta"), F.lit(0)))
        # Missing lengths must stay unknown, rather than becoming a zero change.
        .withColumn("bytes_added", F.when(F.col("bytes_delta").isNotNull(), F.col("bytes_added")))
        .withColumn("bytes_removed", F.when(F.col("bytes_delta").isNotNull(), F.col("bytes_removed")))
    )


def aggregate_activity(fact, group_columns):
    return fact.groupBy(*group_columns).agg(
        F.sum("edit_count").alias("edit_count"),
        F.countDistinct("editor_key").alias("editor_count"),
        F.countDistinct("page_key").alias("page_count"),
        F.sum(F.coalesce(F.col("bot"), F.lit(False)).cast("long")).alias("bot_edit_count"),
        F.sum("bytes_delta").alias("net_bytes_delta"),
        F.sum("bytes_added").alias("bytes_added"),
        F.sum("bytes_removed").alias("bytes_removed"),
        F.count("bytes_delta").alias("edits_with_lengths"),
    ).withColumn("bot_share_pct", 100.0 * F.col("bot_edit_count") / F.col("edit_count"))
