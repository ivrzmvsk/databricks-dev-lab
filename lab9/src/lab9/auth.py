"""Isolate credentials for an explicitly selected, pinned workspace."""

import os
from configparser import ConfigParser
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

ROOT = Path(__file__).resolve().parents[2]
PERSONAL_HOST = "https://dbc-ab9a5afe-c151.cloud.databricks.com"
BUNDLE_NAME = "lab9_wikipedia_automation"
TARGETS = {
    "personal": {"host": PERSONAL_HOST, "profile": "personal-env"},
    "azure_trial_2": {
        "host": "https://adb-7405618361895520.0.azuredatabricks.net",
        "profile": "azure_trial_2",
    },
}


def target_settings(target):
    if target not in TARGETS:
        raise ValueError("Unknown Lab 9 target")
    return TARGETS[target]


def credentials(path=None):
    # Explicit environment credentials (CI) take precedence over the local file.
    values = {k: v for k, v in os.environ.items() if k.startswith("DATABRICKS_")}
    if path or not values.get("DATABRICKS_TOKEN"):
        source = Path(path) if path else ROOT.parent / "lab8/.env.personal"
        if not source.is_file():
            raise ValueError(
                "Set personal DATABRICKS_HOST/TOKEN or supply --credentials-file"
            )
        values = {}
        for number, line in enumerate(source.read_text().splitlines(), start=1):
            if line.strip() and not line.lstrip().startswith("#"):
                if "=" not in line or not line.split("=", 1)[0].strip():
                    raise ValueError(
                        f"Invalid credentials file at line {number}: expected KEY=VALUE"
                    )
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    if values.get("DATABRICKS_HOST", "").rstrip("/") != PERSONAL_HOST:
        raise ValueError("Lab 9 execution is restricted to the personal workspace")
    if not values.get("DATABRICKS_TOKEN"):
        raise ValueError("Personal Databricks token is missing")
    return values["DATABRICKS_TOKEN"]


@contextmanager
def personal_environment(path=None):
    token = credentials(path)
    with pat_environment(PERSONAL_HOST, "personal-env", token) as environment:
        yield environment


@contextmanager
def pat_environment(host, profile, token):
    environment = {
        k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_")
    }
    with TemporaryDirectory(prefix="lab9-auth-") as directory:
        config = Path(directory) / "databrickscfg"
        config.write_text(f"[{profile}]\nhost = {host}\nauth_type = pat\n")
        config.chmod(0o600)
        environment.update(
            DATABRICKS_HOST=host,
            DATABRICKS_TOKEN=token,
            DATABRICKS_AUTH_TYPE="pat",
            DATABRICKS_CONFIG_PROFILE=profile,
            DATABRICKS_CONFIG_FILE=str(config),
        )
        yield environment


@contextmanager
def isolated_environment(environment):
    """Temporarily isolate SDK configuration in this single-threaded CLI."""
    previous = os.environ.copy()
    try:
        os.environ.clear()
        os.environ.update(environment)
        yield
    finally:
        os.environ.clear()
        os.environ.update(previous)


def personal_client(environment):
    # Resolve SDK configuration without inheriting an unrelated Azure/OAuth profile.
    with isolated_environment(environment):
        config = Config(
            host=PERSONAL_HOST,
            token=environment["DATABRICKS_TOKEN"],
            auth_type="pat",
            config_file=environment["DATABRICKS_CONFIG_FILE"],
            http_timeout_seconds=30,
            retry_timeout_seconds=60,
        )
    if config.host.rstrip("/") != PERSONAL_HOST:
        raise ValueError("Unexpected SDK workspace")
    return WorkspaceClient(config=config)


@contextmanager
def workspace_environment(target="personal", path=None):
    settings = target_settings(target)
    if target == "personal":
        with personal_environment(path) as environment:
            yield environment
        return
    if path:
        raise ValueError(
            "--credentials-file is personal-only; trial 2 uses its profile or explicit PAT environment"
        )
    if os.environ.get("DATABRICKS_AUTH_TYPE") == "pat":
        if os.environ.get("DATABRICKS_HOST", "").rstrip("/") != settings["host"]:
            raise ValueError("Azure PAT host does not match the intended workspace")
        token = os.environ.get("DATABRICKS_TOKEN")
        if not token:
            raise ValueError("Azure Databricks token is missing")
        with pat_environment(
            settings["host"], settings["profile"], token
        ) as environment:
            yield environment
        return
    source = Path.home() / ".databrickscfg"
    profiles = ConfigParser(interpolation=None)
    profiles.read(source)
    profile = settings["profile"]
    if (
        not profiles.has_section(profile)
        or profiles[profile].get("host", "").rstrip("/") != settings["host"]
    ):
        raise ValueError("Azure trial 2 profile does not match the intended workspace")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("DATABRICKS_")
    }
    environment.update(
        DATABRICKS_HOST=settings["host"],
        DATABRICKS_CONFIG_PROFILE=profile,
        DATABRICKS_CONFIG_FILE=str(source),
        DATABRICKS_AUTH_TYPE=profiles[profile].get("auth_type", "databricks-cli"),
    )
    yield environment


def workspace_client(environment, target="personal"):
    settings = target_settings(target)
    if target == "personal":
        return personal_client(environment)
    with isolated_environment(environment):
        config = Config(
            host=settings["host"],
            profile=settings["profile"],
            auth_type=environment["DATABRICKS_AUTH_TYPE"],
            config_file=environment["DATABRICKS_CONFIG_FILE"],
            http_timeout_seconds=30,
            retry_timeout_seconds=60,
        )
        client = WorkspaceClient(config=config)
    if config.host.rstrip("/") != settings["host"]:
        raise ValueError("Unexpected Azure workspace")
    return client
