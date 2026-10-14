"""Offline checks for resource scope, sync boundaries and the release DAG."""

import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def main():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    targets = bundle["targets"]
    assert targets["dev"]["default"] is True
    assert targets["dev"]["workspace"]["profile"] == "personal-env"
    assert targets["prod"]["workspace"]["profile"] == "azure_trial_2"
    assert "dataset/games_*.csv" in bundle["sync"]["exclude"]
    assert "dataset/board_games.json" in bundle["sync"]["include"]
    for artifact in ("databricks.yml", "resources/**"):
        assert artifact in bundle["sync"]["include"]
    job = yaml.safe_load((ROOT / "resources/steam.job.yml").read_text())["resources"]["jobs"][
        "steam_rag"
    ]
    assert not any(k in job for k in ("schedule", "continuous", "trigger"))
    assert job["max_concurrent_runs"] == 1
    tasks = job["tasks"]
    assert [t["task_key"] for t in tasks] == ["ingest", "index", "evaluate"]
    for previous, current in zip(tasks, tasks[1:], strict=False):
        assert current["depends_on"] == [{"task_key": previous["task_key"]}]
    expected_notebooks = ["01_ingest.py", "02_sync_index.py", "03_evaluate.py"]
    for task, expected_notebook in zip(tasks, expected_notebooks, strict=True):
        assert task["max_retries"] == 0
        notebook = (ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve()
        assert notebook.is_relative_to(ROOT) and notebook.is_file()
        assert notebook.name == expected_notebook
        tree = ast.parse(notebook.read_text())
        assert not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"create_endpoint", "create_index"}
            for node in ast.walk(tree)
        ), "Infrastructure creation must stay outside the data Job"
    assert not (ROOT / "src/lab12/runtime.py").exists()
    for directory in ("src", "tools", "tests", "notebooks"):
        for path in (ROOT / directory).rglob("*.py"):
            ast.parse(path.read_text(), filename=str(path))
    print("Bundle scope, sync exclusions, dependency DAG and Python syntax: PASS")


if __name__ == "__main__":
    main()
