"""Write a host-only DEV profile for GitHub Actions; tokens stay in the environment."""
import os
from pathlib import Path


def main():
    if not os.environ.get('DATABRICKS_TOKEN'):
        raise SystemExit('Configure DATABRICKS_DEV_TOKEN in GitHub Secrets')
    config = Path(os.environ['RUNNER_TEMP']) / 'lab8-databrickscfg'
    config.write_text(
        '[personal-env]\nhost = https://dbc-ab9a5afe-c151.cloud.databricks.com\n'
    )
    config.chmod(0o600)
    with open(os.environ['GITHUB_ENV'], 'a') as environment:
        environment.write(f'DATABRICKS_CONFIG_FILE={config}\n')


if __name__ == '__main__':
    main()
