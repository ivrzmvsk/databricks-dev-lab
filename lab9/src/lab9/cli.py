"""Explicitly selected personal or Azure trial 2 deployment and execution."""

import argparse
import fcntl
import json
import subprocess
import sys

from databricks.sdk.errors import DatabricksError

from .auth import ROOT, TARGETS, workspace_client, workspace_environment
from .automation import Automation
from .deployment import bundle_command, read_inventory, save_inventory
from .reporting import emit, job_snapshot


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def validate(args, environment):
    print(bundle_command("validate", environment, args.target))
    return 0


def inventory(args, environment):
    summary = json.loads(bundle_command("summary", environment, args.target))
    emit(save_inventory(summary, args.inventory, args.target))
    return 0


def deploy(args, environment):
    validate(args, environment)
    print(bundle_command("deploy", environment, args.target))
    return inventory(args, environment)


def status(args, environment):
    client = workspace_client(environment, args.target)
    emit(job_snapshot(client.jobs.get_run(run_id=args.run_id)))
    return 0


def run_automation(args, environment):
    resources = read_inventory(args.inventory, args.target)
    # Local invocations share a lock; CI also uses a concurrency group.
    lock_path = ROOT / ".state/automation.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                "Another local Lab 9 operation is already running"
            ) from None
        client = workspace_client(environment, args.target)
        return Automation(
            client,
            resources,
            args.report,
            args.timeout,
            args.poll_interval,
            target=args.target,
        ).execute(args.command, args.commit)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Lab 9 Wikipedia automation (explicit workspace selection)"
    )
    parser.add_argument(
        "--credentials-file", help="Personal PAT file; defaults to lab8/.env.personal"
    )
    parser.add_argument("--target", choices=TARGETS, default="personal")
    parser.add_argument("--inventory")
    sub = parser.add_subparsers(dest="command", required=True)
    for command, handler in (
        ("validate", validate),
        ("deploy", deploy),
        ("inventory", inventory),
    ):
        sub.add_parser(command).set_defaults(func=handler)
    for command in ("run", "pipeline", "submit-notebook"):
        operation = sub.add_parser(command)
        operation.set_defaults(func=run_automation)
        operation.add_argument("--timeout", type=positive, default=3600)
        operation.add_argument("--poll-interval", type=positive, default=15)
        operation.add_argument("--report", default=str(ROOT / "reports/latest.json"))
        operation.add_argument("--commit", default="local")
    operation = sub.add_parser("status")
    operation.set_defaults(func=status)
    operation.add_argument("run_id", type=positive)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.inventory is None:
        args.inventory = str(ROOT / f".state/{args.target}-inventory.json")
    try:
        with workspace_environment(args.target, args.credentials_file) as environment:
            return args.func(args, environment)
    except (
        ValueError,
        KeyError,
        OSError,
        RuntimeError,
        DatabricksError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ) as error:
        message = " ".join(str(error).splitlines())
        print(f"Lab 9: {message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
