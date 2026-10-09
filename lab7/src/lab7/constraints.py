"""Shared fact contract and pure helpers for setup and negative-write tests."""

from lab7.quality import FACT_RULES, classify, make_fact

BRONZE_SCHEMA = (
    "event_json STRING, kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, "
    "kafka_enqueued_at TIMESTAMP, _ingested_at TIMESTAMP"
)
FACT_NOT_NULL_COLUMNS = {
    "event_id",
    "wiki_key",
    "date_key",
    "page_key",
    "editor_key",
    "namespace_key",
    "edit_count",
}
FACT_CONSTRAINTS = {
    "one_edit": "edit_count = 1",
    "lengths_nonnegative": (
        "(old_length IS NULL OR old_length >= 0) AND (new_length IS NULL OR new_length >= 0)"
    ),
    "bytes_consistent": FACT_RULES["bytes_consistent"],
}


def empty_fact_schema(spark):
    """Derive the schema without reading data or requiring pipeline-owned tables."""
    return make_fact(classify(spark.createDataFrame([], BRONZE_SCHEMA))).schema


def fact_ddl(schema):
    return ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}"
        + (" NOT NULL" if field.name in FACT_NOT_NULL_COLUMNS else "")
        for field in schema.fields
    )


def constraint_error_condition(exc):
    for accessor in ("getCondition", "getErrorClass"):
        method = getattr(exc, accessor, None)
        if method:
            condition = method()
            if condition:
                return condition
    return None


def is_expected_constraint_error(condition, column, parameters=None):
    if column == "event_id":
        return condition == "DELTA_NOT_NULL_CONSTRAINT_VIOLATED"
    constraint = {
        "edit_count": "one_edit",
        "old_length": "lengths_nonnegative",
        "bytes_delta": "bytes_consistent",
    }.get(column)
    if constraint:
        return (parameters or {}).get("constraintName") == constraint and condition in {
            "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
            "DELTA_VIOLATE_CONSTRAINT_WITHOUT_VALUES",
            "DELTA_CHECK_CONSTRAINT_VIOLATED",
        }
    return False


NEGATIVE_WRITES = (
    ("not_null_rejected", "CAST(NULL AS STRING)", "event_id"),
    ("check_rejected", "CAST(-1 AS BIGINT)", "edit_count"),
    ("lengths_nonnegative_rejected", "CAST(-1 AS BIGINT)", "old_length"),
    (
        "bytes_consistent_rejected",
        "CAST(coalesce(bytes_added, 0) - coalesce(bytes_removed, 0) + 1 AS BIGINT)",
        "bytes_delta",
    ),
)
