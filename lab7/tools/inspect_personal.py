"""Inspect quarantine, DQ reports and enforced constraints in personal-env."""
import json
from pathlib import Path
import sys

repo = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(repo / "lab7/src"))

from databricks.connect import DatabricksSession
from lab7.config import Config


def main():
    spark = DatabricksSession.builder.profile("personal-env").serverless().getOrCreate()
    config = Config()
    manifest = spark.table(config.table("snapshot_manifest")).first()
    checks = spark.table(config.table("dq_results")).filter(
        f"snapshot_id = '{manifest.snapshot_id}'"
    )
    report = {
        "manifest": json.loads(manifest.metadata_json),
        "mandatory_failures": checks.filter("severity = 'error' AND NOT passed").count(),
        "snapshot_age_policy": checks.filter("check_name = 'snapshot_processing_freshness'")
            .select("severity", "passed", "details").first().asDict(),
        "dimensions": [row.asDict() for row in checks.groupBy("dimension").count().collect()],
        "quarantine": [row.asDict() for row in spark.table(config.table("wiki_quarantine"))
                       .select("event_id", "kafka_offset", "_quality_errors").orderBy("kafka_offset").collect()],
        "quarantine_demo": [row.asDict() for row in spark.table(config.table("wiki_quarantine_demo"))
                            .select("event_id", "event_json", "kafka_offset", "_quality_errors", "demo_run_id")
                            .orderBy("kafka_offset").collect()],
        "dqx_fact_quarantine_rows": spark.table(config.table("dqx_fact_quarantine")).count(),
        "real_bronze_columns": spark.table(config.table("wiki_bronze")).columns,
        "fixture_event_ids_in_real_fact": spark.table(config.table("fact_edits")).filter(
            "event_id LIKE 'lab7-fixture-%' OR event_id LIKE 'lab5-fixture-%'"
        ).count(),
        "duplicates": [row.asDict() for row in spark.table(config.table("wiki_duplicates"))
                       .select("event_id", "kafka_offset", "_duplicate_reason").collect()],
        "healthy_fact_rows_after_negative_test": spark.table(config.table("fact_edits")).count(),
        "not_null_columns": [field.name for field in spark.table(config.table("fact_edits")).schema.fields if not field.nullable],
        "check_constraints": {row[0]: row[1] for row in spark.sql(f"SHOW TBLPROPERTIES {config.table('fact_edits')}").collect()
                              if row[0].startswith("delta.constraints.")},
        "source_counts": {name: spark.table(config.source(name, gold)).count()
                          for name, gold in [("wiki_bronze_lab5", False), ("wiki_silver_lab5", False),
                                             ("wiki_quarantine_lab5", False), ("fact_edits", True)]},
    }
    spark.stop()
    output = repo / "lab7/evidence/inspection.json"
    output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    if report["mandatory_failures"]:
        raise AssertionError("Mandatory DQ failures exist")
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    main()
