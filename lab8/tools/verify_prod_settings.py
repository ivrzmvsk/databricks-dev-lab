"""Check CI-supplied PROD settings locally, without contacting a workspace."""
import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def main():
    required = ['DATABRICKS_HOST', 'DATABRICKS_TOKEN']
    if os.environ.get('DATABRICKS_AUTH_TYPE') != 'pat':
        raise SystemExit('This workflow uses PAT authentication')
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise SystemExit('Missing GitHub settings: ' + ', '.join(missing))
    bundle = yaml.safe_load((ROOT / 'databricks.yml').read_text())
    expected = bundle['targets']['prod']['workspace']['host'].rstrip('/')
    if os.environ['DATABRICKS_HOST'].rstrip('/') != expected:
        raise SystemExit('PROD host does not match the intended Azure workspace')


if __name__ == '__main__':
    main()
