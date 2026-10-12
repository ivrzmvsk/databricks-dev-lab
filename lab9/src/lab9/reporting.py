"""Versioned operation reports and SDK state snapshots."""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .auth import PERSONAL_HOST


def emit(value):
    print(json.dumps(value, default=str), flush=True)


def state_name(value):
    return getattr(value, "value", value)


def job_snapshot(run):
    state = run.state
    return {
        "run_id": run.run_id,
        "state": state_name(state.life_cycle_state) if state else "UNKNOWN",
        "result": state_name(state.result_state) if state else None,
        "message": state.state_message if state else None,
        "url": run.run_page_url,
        "tasks": [
            {
                "task": task.task_key,
                "run_id": task.run_id,
                "state": state_name(task.state.life_cycle_state)
                if task.state
                else "UNKNOWN",
                "result": state_name(task.state.result_state) if task.state else None,
            }
            for task in run.tasks or []
        ],
    }


def pipeline_snapshot(update):
    return {"state": state_name(update.state) if update else "UNKNOWN"}


@dataclass
class Report:
    path: Path
    host: str = PERSONAL_HOST
    target: str = "personal"
    data: dict = field(init=False)

    def __post_init__(self):
        self.path = Path(self.path)
        self.data = {
            "format_version": 3,
            "operation_id": uuid4().hex,
            "host": self.host,
            "target": self.target,
            "started_at": datetime.now(UTC).isoformat(),
            "status": "RUNNING",
            "stages": [],
        }

    def save(self, elapsed):
        self.data["duration_seconds"] = round(elapsed, 3)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.data, indent=2, default=str) + "\n")
        temporary.replace(self.path)

    def finish(self):
        self.data["finished_at"] = datetime.now(UTC).isoformat()

    def emit_summary(self):
        emit(
            {
                "status": self.data["status"],
                "report": str(self.path),
                "duration_seconds": self.data["duration_seconds"],
                **({"error": self.data["error"]} if "error" in self.data else {}),
            }
        )
