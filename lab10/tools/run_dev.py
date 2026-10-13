"""Run the full lab through the DEV SQL warehouse and save reviewable evidence."""

import json
import sys
from datetime import datetime, timezone

from common import ROOT, sql_client

sys.path.insert(0, str(ROOT / "src"))
from lab10.notebook_runner import run_notebook  # noqa: E402
from lab10.pipeline import Pipeline  # noqa: E402


def main():
    p = Pipeline(sql_client())
    report = {"environment": "personal-env", "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        report["setup"] = run_notebook("00_setup", p)
        print("Setup: PASS", flush=True)
        report["federation"] = run_notebook("01_federation", p)
        print("Foreign catalog, Delta comparison and hybrid join: PASS", flush=True)
        report["batch1"] = p.simulate("batch1")
        if p.latest() > p.state():
            before = p.state()
            try:
                p.sync(fail_after_history_close=True)
            except RuntimeError as exc:
                if not str(exc).startswith("Injected failure"):
                    raise
            else:
                raise AssertionError("Failure injection did not run")
            assert p.state() == before, "Checkpoint advanced after injected failure"
            report["failure_recovery"] = {"checkpoint_unchanged": True}
        else:
            report["failure_recovery"] = {"status": "SKIPPED_ALREADY_CONSUMED"}
        report["sync1"] = p.sync()
        print("First batch and recovery: PASS", flush=True)
        report["batch2"] = p.simulate("batch2")
        report["sync2"] = p.sync()
        print("Second batch, intermediate history and late business timestamp: PASS", flush=True)
        checks = run_notebook("03_validate", p)
        report["rerun"] = checks["rerun"]
        report["validation"] = checks["validation"]
        report["status"] = "PASS"
        print(json.dumps(report["validation"]), flush=True)
    except Exception as exc:
        report["status"] = "FAIL"
        report["error_type"] = type(exc).__name__
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        directory = ROOT / "reports"
        directory.mkdir(exist_ok=True)
        (directory / "dev-e2e.json").write_text(json.dumps(report, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
