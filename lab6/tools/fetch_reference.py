"""Fetch public references locally when Free Edition blocks outbound API requests.

Pass --wikis with the observed wiki IDs. This script does not use credentials.
"""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from lab6.metadata import fetch_snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wikis', nargs='+', required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'assets' / 'wikimedia.json')
    args = parser.parse_args()
    snapshot = fetch_snapshot(set(args.wikis))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'sites': len(snapshot['sites']), 'namespace_maps': len(snapshot['namespaces']),
                      'namespace_errors': snapshot['namespace_errors'], 'output': str(args.output)}))


if __name__ == '__main__':
    main()
