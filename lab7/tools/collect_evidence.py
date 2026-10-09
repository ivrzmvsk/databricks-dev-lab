"""Read-only personal-env CLI evidence collection; never selects an Azure profile."""
import argparse
import json
from pathlib import Path
import subprocess


def cli(*arguments):
    result = subprocess.run(
        ["databricks", *arguments, "--profile", "personal-env", "-o", "json"],
        check=True, text=True, capture_output=True,
    )
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id", type=int)
    parser.add_argument("--output", type=Path, default=Path("lab7/evidence/headless.json"))
    args = parser.parse_args()
    run = cli("jobs", "get-run", str(args.run_id))
    report = {"run_id": run["run_id"], "url": run.get("run_page_url"),
              "state": run.get("state"), "tasks": []}
    for task in run.get("tasks", []):
        item = {"key": task["task_key"], "run_id": task["run_id"], "state": task.get("state")}
        if task.get("state", {}).get("life_cycle_state") in {"TERMINATED", "INTERNAL_ERROR"} and "notebook_task" in task:
            output = cli("jobs", "get-run-output", str(task["run_id"]))
            text = output.get("notebook_output", {}).get("result")
            if text:
                try:
                    item["output"] = json.loads(text)
                except json.JSONDecodeError:
                    item["output"] = text
            if output.get("error"):
                item["error"] = output["error"]
        report["tasks"].append(item)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
