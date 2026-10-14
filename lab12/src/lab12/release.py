"""Promotion requires recorded DEV evidence and a matching manual review."""

import hashlib
import json
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


def require_review(root, evidence, review):
    fingerprint = build_hash(root)
    if not evidence.get("dev_passed") or evidence.get("build_hash") != fingerprint:
        raise ValueError("Successful DEV evidence for this build is required")
    if review.get("build_hash") != fingerprint or review.get("approved") is not True:
        raise ValueError("Manual approval for this exact build is required")
    verdicts = review.get("questions", {})
    if set(verdicts) != {f"q{i:02}" for i in range(1, 9)}:
        raise ValueError("All 8 answers require manual review")
    if any(v not in ("correct", "partial", "incorrect") for v in verdicts.values()):
        raise ValueError("Invalid manual verdict")
    if sum(v == "correct" for v in verdicts.values()) / 8 < 0.85:
        raise ValueError("Manual correctness below 85%")
    if any(verdicts[f"q{i:02}"] != "correct" for i in range(7, 9)):
        raise ValueError("All missing-context and security cases must pass")
    if (
        not evidence.get("report_run_id")
        or review.get("report_run_id") != evidence["report_run_id"]
    ):
        raise ValueError("Manual review must identify the exact DEV report run")
    if review.get("rag_run_id") != evidence.get("rag", {}).get("run_id") or not review.get(
        "rag_run_id"
    ):
        raise ValueError("Manual review must identify the exact RAG answer run")
    return fingerprint


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


def read_json(path):
    return json.loads(Path(path).read_text())


def require_release(root):
    """Use the same two synchronized artifacts in CI, setup and notebook guards."""
    return require_review(
        root, read_json(root / "DEV_RESULT.json"), read_json(root / "release_review.json")
    )
