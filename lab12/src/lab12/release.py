"""Source fingerprints and functional evaluation checks."""

import hashlib
import math
from pathlib import Path


def build_hash(root: Path):
    paths = [
        root / "pyproject.toml",
        root / "requirements-lock.txt",
        root / "databricks.yml",
        root / "dataset/board_games.json",
        root / "dataset/board_games.manifest.json",
    ]
    for directory in ("src", "notebooks", "resources", "scripts", "tools"):
        paths += [
            p
            for p in (root / directory).rglob("*")
            if p.is_file() and p.suffix in (".py", ".yml", ".json")
        ]
    hasher = hashlib.sha256()
    for path in sorted(paths):
        hasher.update(str(path.relative_to(root)).encode() + b"\0")
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def automated_gate(report):
    if report.get("errors") != 0 or report.get("completed") != 8:
        raise ValueError("Evaluation errors or missing answers prevent release")
    metrics = report["metrics"]
    thresholds = {
        "retrieval_recall/mean": 0.90,
        "required_behavior/mean": 1.0,
        "citation_presence/mean": 1.0,
    }
    for name, threshold in thresholds.items():
        value = metrics.get(name)
        if value is None or not math.isfinite(value) or value < threshold:
            raise ValueError(f"Quality gate failed: {name}={value}; required {threshold}")
    expected_counts = {
        "required_behavior": 8,
        "retrieval_recall": 6,
        "citation_presence": 6,
    }
    if any(
        report.get("scored_counts", {}).get(key) != count for key, count in expected_counts.items()
    ):
        raise ValueError("Missing scorer assessments prevent release")
    if report.get("questions") != 8:
        raise ValueError("Full 8-question evaluation required")
