"""Inspect a selected workspace run and store terminal notebook results locally."""

import argparse
import json

from common import ROOT, TARGETS, workspace_client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id", type=int)
    parser.add_argument("--target", choices=TARGETS, default="personal")
    args = parser.parse_args()
    client = workspace_client(args.target)
    run = client.jobs.get_run(args.run_id)
    report = {
        "environment": args.target,
        "run_id": run.run_id,
        "url": run.run_page_url,
        "state": run.state.as_dict() if run.state else {},
        "tasks": [],
    }
    terminal = report["state"].get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED")
    for task in run.tasks or []:
        item = {
            "task": task.task_key,
            "run_id": task.run_id,
            "state": task.state.as_dict() if task.state else {},
        }
        report["tasks"].append(item)
    print(json.dumps(report, indent=2))
    if terminal:
        for item in report["tasks"]:
            if not item["run_id"]:
                continue
            output = client.jobs.get_run_output(item["run_id"])
            if output.notebook_output and output.notebook_output.result:
                item["output"] = json.loads(output.notebook_output.result)
            if output.error:
                item["error"] = output.error
        directory = ROOT / "reports"
        directory.mkdir(exist_ok=True)
        (directory / f"job-{run.run_id}.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
        if report["state"].get("result_state") != "SUCCESS":
            raise SystemExit("Job failed; inspect the local report")


if __name__ == "__main__":
    main()
