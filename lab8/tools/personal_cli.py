"""Run Databricks CLI with the ignored personal token file, without shell sourcing."""
import os
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERSONAL_HOST = 'https://dbc-ab9a5afe-c151.cloud.databricks.com'


@contextmanager
def personal_environment():
    values = {}
    for line in (ROOT / '.env.personal').read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    if values.get('DATABRICKS_HOST', '').rstrip('/') != PERSONAL_HOST:
        raise ValueError('Personal credentials file points to an unexpected workspace')
    if not values.get('DATABRICKS_TOKEN') or values.get('DATABRICKS_AUTH_TYPE') != 'pat':
        raise ValueError('Configure the personal access token in .env.personal')
    environment = {k: v for k, v in os.environ.items() if not k.startswith('DATABRICKS_')}
    environment.update({key: values[key] for key in
                        ('DATABRICKS_HOST', 'DATABRICKS_TOKEN', 'DATABRICKS_AUTH_TYPE')})
    # Avoid the expired OAuth profile; the temporary file never contains a token.
    with tempfile.TemporaryDirectory(prefix='lab8-personal-') as directory:
        config = Path(directory) / 'databrickscfg'
        config.write_text(f'[personal-env]\nhost = {PERSONAL_HOST}\nauth_type = pat\n')
        config.chmod(0o600)
        environment['DATABRICKS_CONFIG_FILE'] = str(config)
        environment['DATABRICKS_CONFIG_PROFILE'] = 'personal-env'
        yield environment


def main():
    arguments = sys.argv[1:]
    if not arguments:
        raise SystemExit('Usage: python tools/personal_cli.py <Databricks CLI arguments>')
    for index, argument in enumerate(arguments):
        if argument in ('--profile', '-p', '--target', '-t', '--host'):
            raise SystemExit('Profile, target and host are fixed by this personal-only helper')
        if argument.startswith(('--profile=', '--target=', '--host=')):
            raise SystemExit('Profile, target and host are fixed by this personal-only helper')
    arguments += ['--profile', 'personal-env']
    if arguments[0] == 'bundle':
        arguments += ['--target', 'dev']
    with personal_environment() as environment:
        raise SystemExit(subprocess.call(['databricks', *arguments], cwd=ROOT, env=environment))


if __name__ == '__main__':
    main()
