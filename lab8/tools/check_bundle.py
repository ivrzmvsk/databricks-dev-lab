"""Check both target definitions against the CLI schema without workspace access.

This complements, rather than replaces, authenticated bundle validate in CI.
"""
import ast
import json
import subprocess
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).resolve().parents[1]


def python_patterns(value):
    # CLI schema uses Go Unicode regex classes. These substitutions are sufficient
    # for the ASCII substitution identifiers used by this project.
    if isinstance(value, dict):
        if 'pattern' in value:
            value['pattern'] = (value['pattern'].replace(r'[\p{L}\p{N}]', '[A-Za-z0-9]')
                                .replace(r'\p{L}', '[A-Za-z]').replace(r'\p{N}', '[0-9]'))
        for item in value.values():
            python_patterns(item)
    elif isinstance(value, list):
        for item in value:
            python_patterns(item)


def main():
    schema = json.loads(subprocess.check_output(['databricks', 'bundle', 'schema'], text=True))
    python_patterns(schema)
    config = yaml.safe_load((ROOT / 'databricks.yml').read_text())
    for path in sorted((ROOT / 'resources').glob('*.yml')):
        resource = yaml.safe_load(path.read_text())
        for kind, values in resource['resources'].items():
            config.setdefault('resources', {}).setdefault(kind, {}).update(values)
    errors = list(jsonschema.Draft202012Validator(schema).iter_errors(config))
    if errors:
        for error in errors:
            print('/'.join(map(str, error.path)), error.message[:500])
        raise SystemExit('Bundle does not match the installed CLI schema')
    for path in ROOT.rglob('*.py'):
        ast.parse(path.read_text(), filename=str(path))
    for path in (ROOT / 'assets').glob('*.json'):
        json.loads(path.read_text())
    dashboard = json.loads((ROOT / 'dashboards/wikimedia_activity.lvdash.json').read_text())
    for dataset in dashboard['datasets']:
        sql = (ROOT / f"sql/dashboard/{dataset['name']}.sql").read_text()
        assert sql.strip() == ''.join(dataset['queryLines']).strip(), dataset['name']
    print('CLI schema, Python syntax, reference assets and dashboard SQL: passed (offline)')


if __name__ == '__main__':
    main()
