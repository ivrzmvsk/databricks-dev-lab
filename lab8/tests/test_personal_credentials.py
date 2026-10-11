import pytest

import personal_cli


def test_personal_loader_isolates_profile_and_keeps_token_out_of_config(tmp_path, monkeypatch):
    monkeypatch.setattr(personal_cli, 'ROOT', tmp_path)
    monkeypatch.setenv('DATABRICKS_HOST', 'https://wrong-workspace.example')
    monkeypatch.setenv('DATABRICKS_CLIENT_SECRET', 'unrelated-credential')
    (tmp_path / '.env.personal').write_text(
        f'DATABRICKS_HOST={personal_cli.PERSONAL_HOST}\n'
        'DATABRICKS_AUTH_TYPE=pat\nDATABRICKS_TOKEN=synthetic-test-token\n')
    with personal_cli.personal_environment() as environment:
        from pathlib import Path

        assert environment['DATABRICKS_HOST'] == personal_cli.PERSONAL_HOST
        assert 'DATABRICKS_CLIENT_SECRET' not in environment
        config = Path(environment['DATABRICKS_CONFIG_FILE'])
        assert 'synthetic-test-token' not in config.read_text()
        assert config.stat().st_mode & 0o777 == 0o600
    assert not config.exists()


def test_personal_loader_rejects_another_workspace(tmp_path, monkeypatch):
    monkeypatch.setattr(personal_cli, 'ROOT', tmp_path)
    (tmp_path / '.env.personal').write_text(
        'DATABRICKS_HOST=https://wrong-workspace.example\n'
        'DATABRICKS_AUTH_TYPE=pat\nDATABRICKS_TOKEN=synthetic-test-token\n')
    with pytest.raises(ValueError, match='unexpected workspace'):
        with personal_cli.personal_environment():
            pytest.fail('A personal-only command must not accept another workspace')
