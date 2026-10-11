"""Guard environment boundaries and destructive streaming options in CI."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def read(name):
    return yaml.safe_load((ROOT / name).read_text())


def test_environment_and_compute_boundaries():
    bundle = read('databricks.yml')
    assert set(bundle['targets']) == {'dev', 'prod'}
    assert bundle['targets']['dev']['workspace']['profile'] == 'personal-env'
    from urllib.parse import urlparse
    host = urlparse(bundle['targets']['prod']['workspace']['host'])
    assert host.scheme == 'https' and host.hostname.endswith('.azuredatabricks.net')
    assert not host.path and not host.username and not host.password
    assert bundle['targets']['prod']['mode'] == 'production'
    assert bundle['variables']['silver_schema']['default'] != bundle['variables']['gold_schema']['default']
    for name in ('silver_schema', 'gold_schema'):
        assert '_lab8_' in bundle['variables'][name]['default']
    job = read('resources/promotion.job.yml')['resources']['jobs']['promotion_job']
    assert not any(key in job for key in ('schedule', 'continuous', 'trigger'))
    assert job['max_concurrent_runs'] == 1
    prod_resources = bundle['targets']['prod']['resources']
    assert prod_resources['pipelines']['wiki_pipeline']['serverless'] is True
    assert 'clusters' not in prod_resources['pipelines']['wiki_pipeline']
    assert 'jobs' not in prod_resources  # Both targets use the shared serverless Job.
    assert 'job_clusters' not in job
    for task in job['tasks']:
        assert 'job_cluster_key' not in task
        if 'pipeline_task' in task:
            assert task['pipeline_task']['full_refresh'] is False
        if 'notebook_task' in task:
            assert (ROOT / 'resources' / task['notebook_task']['notebook_path']).resolve().is_file()


def test_promotion_cannot_run_on_pull_requests():
    workflow = yaml.safe_load((ROOT.parent / '.github/workflows/lab8-ci.yml').read_text())
    prod = workflow['jobs']['deploy-prod']
    assert "github.event_name == 'push'" in prod['if']
    assert "github.ref == 'refs/heads/main'" in prod['if']
    assert 'vars.' not in str(prod)
    assert prod['env']['DATABRICKS_AUTH_TYPE'] == 'pat'
    assert prod['concurrency']['cancel-in-progress'] is False
    commands = '\n'.join(step.get('run', '') for step in prod['steps'])
    assert 'bundle deploy -t prod' in commands
    assert 'bundle run promotion_job -t prod' in commands
    assert '--no-wait' not in commands
    assert 'python tools/verify_prod_settings.py' in commands
    for name, job in workflow['jobs'].items():
        if name != 'deploy-prod':
            assert 'DATABRICKS_PROD_' not in str(job)
