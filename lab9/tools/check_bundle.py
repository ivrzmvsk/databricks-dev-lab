"""Check both target configurations offline; never authenticate to Azure."""

import ast
import json
import subprocess
import sys
from pathlib import Path

import jsonschema

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent / "lab8/tools"))
from check_bundle import load_bundle_config, python_patterns


def main():
    schema = json.loads(
        subprocess.check_output(["databricks", "bundle", "schema"], text=True)
    )
    python_patterns(schema)
    config = load_bundle_config(ROOT)
    errors = list(jsonschema.Draft202012Validator(schema).iter_errors(config))
    if errors:
        for error in errors:
            print("/".join(map(str, error.path)), error.message[:500])
        raise SystemExit("Bundle does not match the installed CLI schema")
    for directory in ("src", "tools", "notebooks", "tests"):
        for path in (ROOT / directory).rglob("*.py"):
            ast.parse(path.read_text(), filename=str(path))
    print(
        "Lab 9 bundle schema and Python syntax: passed offline (personal + Azure configuration)"
    )


if __name__ == "__main__":
    main()
