from pathlib import Path

import pytest

from lab9.auth import (
    PERSONAL_HOST,
    credentials,
    isolated_environment,
    personal_client,
    personal_environment,
)
from lab9.cli import main
from lab9.deployment import read_inventory


def test_credentials_are_isolated_and_not_written_to_config(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://unrelated.example")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "unrelated-secret")
    source = tmp_path / "credentials"
    source.write_text(
        f"DATABRICKS_HOST={PERSONAL_HOST}\nDATABRICKS_TOKEN=synthetic-token\n"
    )
    with personal_environment(source) as environment:
        assert environment["DATABRICKS_HOST"] == PERSONAL_HOST
        assert "DATABRICKS_CLIENT_SECRET" not in environment
        config = Path(environment["DATABRICKS_CONFIG_FILE"])
        assert "synthetic-token" not in config.read_text()
        assert config.stat().st_mode & 0o777 == 0o600
    assert not config.exists()


def test_azure_credentials_rejected_before_api_use(monkeypatch):
    monkeypatch.setenv(
        "DATABRICKS_HOST", "https://adb-7405618361895520.0.azuredatabricks.net"
    )
    monkeypatch.setenv("DATABRICKS_TOKEN", "synthetic-token")
    assert main(["validate"]) == 1


def test_azure_inventory_is_rejected(tmp_path):
    source = tmp_path / "inventory.json"
    source.write_text('{"target": "azure_trial_2"}')
    with pytest.raises(ValueError, match="personal Lab 9"):
        read_inventory(source)


@pytest.mark.parametrize("invalid", ["sensitive-malformed-line", "=sensitive-value"])
def test_malformed_credentials_report_line_without_leaking_contents(tmp_path, invalid):
    source = tmp_path / "credentials"
    source.write_text(f"# personal credentials\n{invalid}\n")
    with pytest.raises(ValueError, match="line 2: expected KEY=VALUE") as error:
        credentials(source)
    assert invalid not in str(error.value)


def test_isolated_environment_restores_values_after_failure(monkeypatch):
    import os

    monkeypatch.setenv("DATABRICKS_HOST", "https://unrelated.example")
    previous = os.environ.copy()
    with (
        pytest.raises(RuntimeError),
        isolated_environment({"DATABRICKS_HOST": PERSONAL_HOST}),
    ):
        assert dict(os.environ) == {"DATABRICKS_HOST": PERSONAL_HOST}
        raise RuntimeError("configuration failed")
    assert dict(os.environ) == previous


def test_sdk_client_ignores_ambient_credentials_and_restores_environment(
    tmp_path, monkeypatch
):
    import os

    from databricks.sdk import config
    from databricks.sdk.oauth import HostMetadata

    # Config probes host metadata even with explicit PAT auth; keep this test offline.
    monkeypatch.setattr(
        config,
        "get_host_metadata",
        lambda *args, **kwargs: HostMetadata(oidc_endpoint=""),
    )

    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "unrelated-secret")
    monkeypatch.setenv("DATABRICKS_HOST", "https://unrelated.example")
    source = tmp_path / "credentials"
    source.write_text(
        f"DATABRICKS_HOST={PERSONAL_HOST}\nDATABRICKS_TOKEN=synthetic-token\n"
    )
    previous = os.environ.copy()
    with personal_environment(source) as environment:
        client = personal_client(environment)
        assert client.config.host == PERSONAL_HOST
        assert client.config.token == "synthetic-token"
        assert client.config.auth_type == "pat"
        assert not client.config.client_secret
    assert dict(os.environ) == previous


def test_trial_environment_requires_matching_profile_and_drops_ambient_token(
    tmp_path, monkeypatch
):
    from lab9.auth import TARGETS, workspace_environment

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("DATABRICKS_TOKEN", "unrelated-personal-token")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "unrelated-secret")
    source = tmp_path / ".databrickscfg"
    source.write_text("[azure_trial_2]\nhost = https://wrong.example\n")
    with (
        pytest.raises(ValueError, match="intended workspace"),
        workspace_environment("azure_trial_2"),
    ):
        pytest.fail("Wrong host accepted")
    host = TARGETS["azure_trial_2"]["host"]
    source.write_text(f"[azure_trial_2]\nhost = {host}\nauth_type = databricks-cli\n")
    with workspace_environment("azure_trial_2") as environment:
        assert environment["DATABRICKS_HOST"] == host
        assert environment["DATABRICKS_CONFIG_PROFILE"] == "azure_trial_2"
        assert "DATABRICKS_TOKEN" not in environment
        assert "DATABRICKS_CLIENT_SECRET" not in environment


def test_trial_cannot_use_personal_credentials_file():
    from lab9.auth import workspace_environment

    with (
        pytest.raises(ValueError, match="personal-only"),
        workspace_environment("azure_trial_2", "personal.env"),
    ):
        pytest.fail("Personal credentials accepted")


@pytest.mark.parametrize(
    "host,token,message",
    [
        (PERSONAL_HOST, "synthetic-token", "intended workspace"),
        ("https://adb-7405618361895520.0.azuredatabricks.net", "", "token is missing"),
    ],
)
def test_trial_pat_rejects_wrong_host_or_missing_token(
    monkeypatch, host, token, message
):
    from lab9.auth import workspace_environment

    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "pat")
    monkeypatch.setenv("DATABRICKS_HOST", host)
    monkeypatch.setenv("DATABRICKS_TOKEN", token)
    with (
        pytest.raises(ValueError, match=message),
        workspace_environment("azure_trial_2"),
    ):
        pytest.fail("Invalid CI credentials accepted")


def test_trial_ci_pat_needs_no_local_profile_and_is_not_written_to_disk(
    tmp_path, monkeypatch
):
    from databricks.sdk import config
    from databricks.sdk.oauth import HostMetadata

    from lab9.auth import TARGETS, workspace_client, workspace_environment

    monkeypatch.setattr(
        config,
        "get_host_metadata",
        lambda *args, **kwargs: HostMetadata(oidc_endpoint=""),
    )
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    host = TARGETS["azure_trial_2"]["host"]
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "pat")
    monkeypatch.setenv("DATABRICKS_HOST", host)
    monkeypatch.setenv("DATABRICKS_TOKEN", "synthetic-prod-token")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "unrelated-secret")
    with workspace_environment("azure_trial_2") as environment:
        assert "DATABRICKS_CLIENT_SECRET" not in environment
        path = Path(environment["DATABRICKS_CONFIG_FILE"])
        assert "synthetic-prod-token" not in path.read_text()
        assert path.stat().st_mode & 0o777 == 0o600
        client = workspace_client(environment, "azure_trial_2")
        assert client.config.host == host
        assert client.config.token == "synthetic-prod-token"
        assert client.config.profile == "azure_trial_2"
    assert not path.exists()
