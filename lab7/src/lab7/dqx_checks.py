"""DQX adapters for integration tests and the gate; pipeline uses native expectations."""
from pyspark.sql import functions as F


def predicate_checks(rules):
    # DQX sql_expression does not reject SQL NULL by itself: make it explicit.
    return [{"name": name, "criticality": "error", "check": {
        "function": "sql_expression", "arguments": {
            "expression": f"coalesce(({expression}), false)", "name": name,
            "msg": f"Wikipedia contract failed: {name}",
        }}} for name, expression in rules.items()]


def apply_predicates(df, rules, engine):
    return engine.apply_checks_by_metadata(df, predicate_checks(rules))


def classify_source(bronze, engine):
    """DQX adapter; the caller supplies the engine and its execution environment."""
    from lab5.transforms import parse_wiki
    from lab7.quality import ROW_RULES
    checked = apply_predicates(parse_wiki(bronze), ROW_RULES, engine)
    return checked.withColumn("_quality_errors", F.coalesce(
        F.transform("_errors", lambda error: error["name"]), F.array().cast("array<string>")
    )).withColumn("_is_valid", F.col("_errors").isNull())


def trusted_checks(rules, references):
    checks = predicate_checks(rules)
    checks.append({"name": "event_id_unique", "criticality": "error", "check": {
        "function": "is_unique", "arguments": {"columns": ["event_id"]}}})
    for table, key in references.items():
        checks.append({"name": f"{table}_reference", "criticality": "error", "check": {
            "function": "foreign_key", "arguments": {
                "columns": [key], "ref_columns": [key], "ref_df_name": table}}})
    return checks


def apply_trusted(df, rules, reference_keys, reference_frames, engine):
    return engine.apply_checks_by_metadata(
        df.drop("_errors", "_warnings", "_dq_info"), trusted_checks(rules, reference_keys),
        ref_dfs=reference_frames,
    )


def failures_by_rule(checked):
    return {r.name: r["count"] for r in checked.select(
        F.explode("_errors").alias("error")
    ).select(F.col("error.name").alias("name")).groupBy("name").count().collect()}
