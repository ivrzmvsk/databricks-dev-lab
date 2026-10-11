"""Execute each dashboard dataset on personal DEV before dashboard deployment."""
import argparse
import json
import subprocess
from pathlib import Path

from personal_cli import personal_environment

ROOT = Path(__file__).resolve().parents[1]
TABLES = ('fact_edits', 'dim_wiki', 'dim_date', 'dim_page', 'dim_editor', 'dim_namespace',
          'agg_wiki_5min', 'agg_wiki_daily', 'agg_page_activity')


def main():
    import re

    parser = argparse.ArgumentParser()
    parser.add_argument('--warehouse', default='6fec7d20a958edd1')
    args = parser.parse_args()
    with personal_environment() as environment:
        config = json.loads(subprocess.check_output(
            ['databricks', 'bundle', 'validate', '--strict', '--target', 'dev',
             '--profile', 'personal-env', '--output', 'json'], cwd=ROOT, env=environment, text=True))
    schema = config['resources']['schemas']['gold_schema']['name']
    catalog = config['variables']['catalog']['value']
    queries = []
    for path in sorted((ROOT / 'sql/dashboard').glob('*.sql')):
        sql = path.read_text()
        # Dashboard files keep bare tables; qualify only this validation copy.
        for table in TABLES:
            sql = re.sub(r'\b(FROM|JOIN)\s+' + table + r'\b',
                         r'\1 ' + catalog + '.' + schema + '.' + table, sql, flags=re.I)
        queries.append(f'SELECT * FROM ({sql.strip()}) LIMIT 5')
    with personal_environment() as environment:
        result = subprocess.run(['databricks', 'experimental', 'aitools', 'tools', 'query',
                                 '--profile', 'personal-env', '--warehouse', args.warehouse,
                                 '--output', 'json', *queries], text=True, capture_output=True, env=environment)
    if result.returncode:
        raise SystemExit(result.stderr or result.stdout)
    data = json.loads(result.stdout)
    if not isinstance(data, list) or len(data) != len(queries):
        raise SystemExit('Unexpected SQL validation response')
    failures = [r for r in data if r.get('state') != 'SUCCEEDED']
    if failures:
        raise SystemExit(json.dumps(failures, indent=2))
    evidence = ROOT / 'evidence/dashboard-queries.json'
    evidence.write_text(json.dumps(data, indent=2))
    print(f'All {len(queries)} dashboard queries succeeded; evidence: {evidence}')


if __name__ == '__main__':
    main()
