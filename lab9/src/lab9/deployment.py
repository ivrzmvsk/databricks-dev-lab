"""Deploy and discover Lab 9 resources on the selected workspace."""

import json
import subprocess
from pathlib import Path

from .auth import BUNDLE_NAME, ROOT, target_settings


def bundle_command(action, environment, target="personal"):
    settings = target_settings(target)
    if action not in {"validate", "deploy", "summary"}:
        raise ValueError("Unsupported bundle operation")
    command = [
        "databricks",
        "bundle",
        action,
        "--target",
        target,
        "--profile",
        settings["profile"],
    ]
    if action == "validate":
        command.append("--strict")
    if action == "summary":
        command += ["--output", "json"]
    return subprocess.check_output(
        command, cwd=ROOT, env=environment, text=True, timeout=1200
    )


def read_inventory(path, target="personal"):
    settings = target_settings(target)
    inventory = json.loads(Path(path).read_text())
    if (
        inventory.get("host") != settings["host"]
        or inventory.get("target") != target
        or inventory.get("bundle") != BUNDLE_NAME
    ):
        raise ValueError(f"Inventory must describe the {target} Lab 9 deployment")
    for key in ("setup_job_id", "gold_job_id", "pipeline_id", "probe_path"):
        if not inventory.get(key):
            raise ValueError(f"Missing inventory field: {key}")
    return inventory


def save_inventory(summary, path, target="personal"):
    settings = target_settings(target)
    if (
        summary["bundle"]["name"] != BUNDLE_NAME
        or summary["bundle"]["target"] != target
    ):
        raise ValueError("Unexpected bundle summary")
    host = summary["workspace"]["host"].rstrip("/")
    if host != settings["host"]:
        raise ValueError("Unexpected workspace in bundle summary")
    resources = summary["resources"]
    inventory = {
        "bundle": BUNDLE_NAME,
        "target": target,
        "host": host,
        "setup_job_id": int(resources["jobs"]["setup_job"]["id"]),
        "gold_job_id": int(resources["jobs"]["gold_job"]["id"]),
        "pipeline_id": resources["pipelines"]["wiki_pipeline"]["id"],
        "probe_path": summary["workspace"]["file_path"]
        + "/lab9/notebooks/compute_probe",
    }
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(inventory, indent=2) + "\n")
    return inventory
