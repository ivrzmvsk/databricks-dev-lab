"""Run the explicit negative pipeline in personal-env and verify its failure."""

import argparse
import json
from pathlib import Path
import time

from collect_evidence import cli


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("pipeline_id")
    parser.add_argument("--output", type=Path, default=Path("lab7/evidence/expected_failure.json"))
    args = parser.parse_args()
    started = cli("pipelines", "start-update", args.pipeline_id)
    update_id = started["update_id"]
    print("Started negative test update", update_id, flush=True)
    previous = None
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        update = cli("pipelines", "get-update", args.pipeline_id, update_id)["update"]
        state = update["state"]
        if state != previous:
            print("Negative test:", state, flush=True)
            previous = state
        if state in {"FAILED", "COMPLETED", "CANCELED"}:
            break
        time.sleep(20)
    else:
        raise TimeoutError("Negative test did not finish within 15 minutes")
    response = cli("pipelines", "list-pipeline-events", args.pipeline_id, "--limit", "200")
    events = response if isinstance(response, list) else response.get("events", [])
    errors = [
        event
        for event in events
        if event.get("level") == "ERROR" and event.get("origin", {}).get("update_id") == update_id
    ]
    report = {
        "pipeline_id": args.pipeline_id,
        "update_id": update_id,
        "state": state,
        "errors": errors,
        "expected_failure_verified": state == "FAILED" and "one_edit" in json.dumps(errors).lower(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    if not report["expected_failure_verified"]:
        raise AssertionError("Expected one_edit failure was not observed: " + json.dumps(report))
    print("Expected one_edit failure verified", flush=True)


if __name__ == "__main__":
    main()
