"""Poll an explicitly selected personal job run and save its execution evidence."""

import argparse
import json
import subprocess
import time

from personal_cli import ROOT, personal_environment


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id", type=int)
    args = parser.parse_args()
    previous = None
    heartbeat = 0
    with personal_environment() as environment:
        while True:
            run = json.loads(
                subprocess.check_output(
                    [
                        "databricks",
                        "jobs",
                        "get-run",
                        str(args.run_id),
                        "--profile",
                        "personal-env",
                        "--output",
                        "json",
                    ],
                    env=environment,
                    text=True,
                )
            )
            (ROOT / f"evidence/run-{args.run_id}.json").write_text(json.dumps(run, indent=2))
            state = run.get("state", {})
            tasks = {
                t["task_key"]: t.get("state", {}).get(
                    "result_state", t.get("state", {}).get("life_cycle_state")
                )
                for t in run.get("tasks", [])
            }
            current = (state, tasks)
            if current != previous or time.monotonic() - heartbeat > 45:
                print(
                    json.dumps({"run_id": args.run_id, "state": state, "tasks": tasks}), flush=True
                )
                previous = current
                heartbeat = time.monotonic()
            if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                for task in run.get("tasks", []):
                    if task.get("state", {}).get("result_state") == "FAILED":
                        output = subprocess.check_output(
                            [
                                "databricks",
                                "jobs",
                                "get-run-output",
                                str(task["run_id"]),
                                "--profile",
                                "personal-env",
                                "--output",
                                "json",
                            ],
                            env=environment,
                            text=True,
                        )
                        (ROOT / f"evidence/task-{task['run_id']}-error.json").write_text(output)
                        result = json.loads(output)
                        print(
                            json.dumps(
                                {
                                    "task": task["task_key"],
                                    "error": result.get("error"),
                                    "trace": result.get("error_trace", "")[-6000:],
                                }
                            ),
                            flush=True,
                        )
                raise SystemExit(0 if state.get("result_state") == "SUCCESS" else 1)
            time.sleep(15)


if __name__ == "__main__":
    main()
