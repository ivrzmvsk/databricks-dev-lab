"""Verify resource ownership and idle state before execution."""

import re

from .auth import BUNDLE_NAME, target_settings
from .reporting import emit, state_name

JOB_NAMES = {
    "setup_job_id": "lab9-wikipedia-setup-personal",
    "gold_job_id": "lab9-wikipedia-gold-personal",
}
PIPELINE_NAME = "lab9-wikipedia-personal"


def resource_name_matches(actual, expected):
    # Development mode injects this prefix even when name_prefix is empty.
    return bool(
        re.fullmatch(r"(?:\[dev [^\]\r\n]+\] )?" + re.escape(expected), actual or "")
    )


def preflight(client, inventory, target="personal"):
    settings = target_settings(target)
    identity = client.current_user.me().user_name
    for key, name in JOB_NAMES.items():
        name = name.removesuffix("personal") + target
        job = client.jobs.get(job_id=inventory[key])
        if not resource_name_matches(job.settings.name, name):
            raise ValueError(f"Inventory {key} points to an unrelated Job")
        if next(
            iter(client.jobs.list_runs(job_id=inventory[key], active_only=True)), None
        ):
            raise RuntimeError(f"{name} already has an active run")
    pipeline = client.pipelines.get(pipeline_id=inventory["pipeline_id"])
    if not resource_name_matches(
        pipeline.name, PIPELINE_NAME.removesuffix("personal") + target
    ):
        raise ValueError("Inventory points to an unrelated pipeline")
    if state_name(pipeline.state) not in {"IDLE", "FAILED"}:
        raise RuntimeError("Wikipedia pipeline already has work in progress")
    expected = f"/Workspace/Users/{identity}/.bundle/{BUNDLE_NAME}/{target}/files"
    if inventory["probe_path"] != expected + "/lab9/notebooks/compute_probe":
        raise ValueError("Inventory notebook is outside this user's Lab 9 deployment")
    emit(
        {
            "preflight": "OK",
            "identity": identity,
            "host": settings["host"],
            "target": target,
        }
    )
    return identity
