"""Create a host-only profile for CI; credential remains in the environment."""

import os
from pathlib import Path

from lab12.config import DEV_HOST


def main():
    if not os.environ.get("DATABRICKS_TOKEN"):
        raise SystemExit("Configure DATABRICKS_DEV_TOKEN in GitHub secrets")
    path = Path(os.environ["RUNNER_TEMP"]) / "lab12-databrickscfg"
    path.write_text(f"[personal-env]\nhost = {DEV_HOST}\n")
    path.chmod(0o600)
    with Path(os.environ["GITHUB_ENV"]).open("a") as handle:
        handle.write(f"DATABRICKS_CONFIG_FILE={path}\n")


if __name__ == "__main__":
    main()
