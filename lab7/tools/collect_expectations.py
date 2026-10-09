"""Read actual named expectation metrics from the personal pipeline event log."""
import argparse
import json
from pathlib import Path
import sys
import uuid

repo = Path(__file__).resolve().parents[2]
for source in ("lab7/src", "lab5/src", "lab6/src"):
    sys.path.insert(0, str(repo / source))

from databricks.connect import DatabricksSession
from lab7.quality import ROW_RULES


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("update_id")
    args = parser.parse_args()
    update_id = str(uuid.UUID(args.update_id))
    spark = DatabricksSession.builder.profile("personal-env").serverless().getOrCreate()
    try:
        rows = spark.sql(f"""
            SELECT id, timestamp, details
            FROM event_log(TABLE(workspace.ivanrazumovskyi_lab7.wiki_accepted))
            WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'
        """).collect()
        metrics = {}
        events = []
        for row in rows:
            detail = json.loads(row.details)
            quality = detail.get("flow_progress", {}).get("data_quality") or {}
            relevant = [m for m in quality.get("expectations") or []
                        if m.get("dataset", "").split(".")[-1] == "wiki_accepted"]
            if relevant:
                events.append({"id": row.id, "timestamp": str(row.timestamp),
                               "data_quality": quality})
            for metric in relevant:
                target = metrics.setdefault(metric["name"], {"passed_records": 0, "failed_records": 0})
                for key in target:
                    target[key] += int(metric.get(key) or 0)
        report = {"update_id": update_id, "dataset": "wiki_accepted",
                  "action": "expect_all_or_drop", "expectations": metrics, "events": events}
        output = repo / "lab7/evidence/expectations.json"
        output.write_text(json.dumps(report, indent=2) + "\n")
        if set(metrics) != set(ROW_RULES):
            raise AssertionError(f"Missing per-rule expectation metrics: {set(ROW_RULES) - set(metrics)}")
        if any(m["failed_records"] for m in metrics.values()):
            raise AssertionError("Unexpected failed expectations in the real clean snapshot")
        print(json.dumps({"update_id": update_id, "named_expectations": len(metrics), "metrics": metrics}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
