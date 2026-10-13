"""Replay one old source event under a new run ID and verify idempotency."""

import argparse
import json
import time
from uuid import uuid4

from _acceptance import AcceptanceEnvironment

from lab11.common import load_manifest


def main(argv=None, *, environment=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", help="Replay manifest; defaults to the committed Wikimedia sample")
    args = parser.parse_args(argv)
    env = environment or AcceptanceEnvironment()
    manifest = load_manifest(args.manifest)
    record = manifest["records"][0]
    event_id = record["event_id"].replace("'", "''")
    env.process()
    query = f"SELECT * FROM {env.silver} WHERE event_id = '{event_id}'"
    if not env.execute(query):
        seed = env.send([record], manifest["run_id"])
        env.wait_rows(f"(SELECT * FROM {env.bronze} WHERE batch_id = '{seed['batch_id']}')", 1)
        env.process()
    before = env.execute(query)
    assert len(before) == 1
    count_before = int(env.execute(f"SELECT COUNT(*) FROM {env.silver}")[0][0])
    report = env.send([record], "late-" + uuid4().hex)
    batch_id = report["batch_id"]
    deadline = time.monotonic() + 120
    while int(env.execute(f"SELECT COUNT(*) FROM {env.bronze} WHERE batch_id = '{batch_id}'")[0][0]) < 1:
        if time.monotonic() >= deadline:
            raise TimeoutError("Late duplicate not queryable")
        time.sleep(2)
    env.process()
    after = env.execute(query)
    assert len(after) == 1 and before[0][:-1] == after[0][:-1]
    assert int(after[0][-1]) == int(before[0][-1]) + 1
    assert int(env.execute(f"SELECT COUNT(*) FROM {env.silver}")[0][0]) == count_before
    env.process()
    assert env.execute(query) == after
    result = {
        "late_duplicate_under_new_run_id": True,
        "business_and_first_delivery_unchanged": True,
        "delivery_count_incremented_once": True,
        "silver_total_events": count_before,
        "repeat_merge_unchanged": True,
        "batch_id": batch_id,
    }
    env.save_report(f"late-{batch_id}.json", result)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
