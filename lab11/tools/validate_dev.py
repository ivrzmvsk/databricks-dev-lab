"""Check payloads and conflicts before MERGE, then prove repeat processing is a no-op."""

import argparse
import json
import time
from pathlib import Path

from _acceptance import AcceptanceEnvironment

from lab11.common import BUSINESS_COLUMNS


def main(argv=None, *, environment=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--min-deliveries", type=int, required=True)
    args = parser.parse_args(argv)
    env = environment or AcceptanceEnvironment()
    manifest = json.loads(Path(args.manifest).read_text())
    records = manifest["records"]
    ids = [r["event_id"] for r in records]
    # Escaped literals, not interpolated arbitrary SQL identifiers.
    literals = ",".join("'" + x.replace("'", "''") + "'" for x in ids)
    condition = f"event_id IN ({literals})"
    deadline = time.monotonic() + 120
    while True:
        rows = env.execute(f"SELECT COUNT(*), COUNT(DISTINCT event_id) FROM {env.bronze} WHERE {condition}")
        deliveries, unique = map(int, rows[0])
        if deliveries >= args.min_deliveries and unique == len(ids):
            break
        if time.monotonic() > deadline:
            raise TimeoutError("Expected events not queryable within 120 seconds")
        time.sleep(2)
    fields = ",".join(
        c if c != "event_timestamp" else "unix_micros(event_timestamp)" for c in BUSINESS_COLUMNS
    )
    expected = {
        tuple(str(r[c]).lower() if isinstance(r[c], bool) else str(r[c]) for c in BUSINESS_COLUMNS)
        for r in records
    }
    actual = {
        tuple(row) for row in env.execute(f"SELECT DISTINCT {fields} FROM {env.bronze} WHERE {condition}")
    }
    if actual != expected:
        raise ValueError("Bronze event payloads differ from manifest")
    env.process()
    silver_rows = env.execute(f"SELECT {fields} FROM {env.silver} WHERE {condition}")
    if len(silver_rows) != len(records) or {tuple(r) for r in silver_rows} != expected:
        raise ValueError("Silver uniqueness/payload check failed")
    before = env.execute(f"SELECT * FROM {env.silver} WHERE {condition} ORDER BY event_id")
    env.process()
    after = env.execute(f"SELECT * FROM {env.silver} WHERE {condition} ORDER BY event_id")
    if before != after:
        raise ValueError("Repeat MERGE changed Silver")
    duplicates = env.execute(
        f"SELECT event_id FROM {env.silver} GROUP BY event_id HAVING COUNT(*) > 1 LIMIT 1"
    )
    if duplicates:
        raise ValueError("Duplicate Silver event_id")
    report = {
        "run_id": manifest["run_id"],
        "bronze_deliveries": deliveries,
        "bronze_unique": unique,
        "silver_events": len(silver_rows),
        "payloads_match_manifest": True,
        "repeat_merge_unchanged": True,
    }
    env.save_report(f"validation-{manifest['run_id']}-{time.time_ns()}.json", report)
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
