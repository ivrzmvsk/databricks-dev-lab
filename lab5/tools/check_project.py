"""Offline syntax, notebook, copied-producer and official bundle-schema checks.

Requires PyYAML, jsonschema and the installed Databricks CLI. No credentials used.
This does not replace remote `databricks bundle validate` or a Lakeflow update.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[1]


def merge(left, right):
    result = deepcopy(left)
    for key, value in right.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        elif isinstance(value, list) and isinstance(result.get(key), list):
            # Generic DAB lists concatenate. CLI handles special keyed lists.
            result[key] = deepcopy(result[key]) + deepcopy(value)
        else:
            result[key] = deepcopy(value)
    return result


def normalize_go_patterns(node):
    # CLI's schema uses Go Unicode categories, unsupported by Python's `re`.
    # All names in this project are ASCII; preserve its variable-reference check.
    if isinstance(node, dict):
        if "pattern" in node and "\\p{" in node["pattern"]:
            node["pattern"] = r"\$\{var(?:\.[A-Za-z_][A-Za-z0-9_-]*(?:\[[0-9]+\])*)+\}"
        for value in node.values():
            normalize_go_patterns(value)
    elif isinstance(node, list):
        for value in node:
            normalize_go_patterns(value)


def main():
    for path in ROOT.rglob("*.py"):
        compile(path.read_text(), str(path), "exec")
    for path in (ROOT / "notebooks").glob("*.ipynb"):
        notebook = json.loads(path.read_text())
        assert notebook["nbformat"] == 4
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                assert cell["outputs"] == []
                source = "".join(cell["source"])
                if source.lstrip().startswith("%run "):
                    target = source.strip().split()[1]
                    assert (path.parent / (target + ".ipynb")).exists()
                else:
                    compile(source, str(path), "exec")
    # The environment template may be customized; producer code/dependencies must match.
    for name in ["producer.py", "requirements.txt"]:
        original = ROOT.parent / "lab3" / "producer" / name
        copied = ROOT / "producer" / name
        assert hashlib.sha256(original.read_bytes()).digest() == hashlib.sha256(copied.read_bytes()).digest(), name
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    for pattern in bundle["include"]:
        for path in ROOT.glob(pattern):
            bundle = merge(bundle, yaml.safe_load(path.read_text()))
    schema = json.loads(subprocess.check_output(["databricks", "bundle", "schema"], text=True))
    normalize_go_patterns(schema)
    jsonschema.validate(bundle, schema)
    assert set(bundle["targets"]) == {"azure_dev", "personal", "azure_trial", "azure_trial_2"}
    for target, config in bundle["targets"].items():
        overrides = deepcopy(config)
        target_variables = overrides.pop("variables", {})
        resolved = merge({key: value for key, value in bundle.items() if key != "targets"}, overrides)
        for name, value in target_variables.items():
            resolved["variables"][name]["default"] = value.get("default") if isinstance(value, dict) else value
        resolved.pop("mode", None)
        resolved.pop("default", None)
        jsonschema.validate(resolved, schema)
        pipelines = resolved["resources"]["pipelines"]
        resource_key = "lab5_pipeline"
        assert set(pipelines) == {resource_key}, f"{target}: unexpected inherited pipeline"
        pipeline = pipelines[resource_key]
        library_names = [Path(item["file"]["path"]).name for item in pipeline["libraries"]]
        expected = ["bronze.py", "silver.py", "quality_demo.py"]
        assert library_names == expected, f"{target}: unexpected or duplicate libraries: {library_names}"
        assert pipeline["edition"] == "ADVANCED"
        assert pipeline["continuous"] is False
        if target in ["personal", "azure_trial", "azure_trial_2"]:
            assert pipeline["serverless"] is True and not pipeline.get("clusters")
        if target in ["azure_trial", "azure_trial_2"]:
            assert "run_as" not in pipeline and "run_as" not in resolved
        print(f"PASS: {target} offline bundle schema")
    print("PASS: Python/notebook syntax and byte-identical Lab 3 producer")


if __name__ == "__main__":
    main()
