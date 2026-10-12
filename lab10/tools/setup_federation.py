"""Provision the explicitly selected workspace with a managed read-only connection."""

import argparse

from common import ROOT, TARGETS, sql_client, workspace_client
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import ConnectionType
from dotenv import dotenv_values

RESOURCES = {
    "personal": ("ivanrazumovskyi_lab10_pg_dev", "ivanrazumovskyi_lab10_neon_dev", ".env.reader"),
    "azure_trial_2": (
        "ivanrazumovskyi_lab10_pg_catalog",
        "ivanrazumovskyi_lab10_neon_prod",
        ".env.reader.prod",
    ),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=TARGETS, default="personal")
    args = parser.parse_args()
    connection, foreign, reader_filename = RESOURCES[args.target]
    client = workspace_client(args.target)
    env = dotenv_values(ROOT / reader_filename)
    required = ["HOST", "PORT", "DATABASE", "USER", "PASSWORD"]
    if any(not env.get("LAB10_READER_" + key) for key in required):
        raise RuntimeError("Run seed_postgres.py first")
    options = {k.lower(): env["LAB10_READER_" + k] for k in ("HOST", "PORT", "USER", "PASSWORD")}
    try:
        existing = client.connections.get(connection)
        if existing.connection_type != ConnectionType.POSTGRESQL or not existing.read_only:
            raise RuntimeError("Existing lab connection has unexpected type or access mode")
        for k in ("host", "port", "user"):
            actual = (existing.options or {}).get(k)
            # UC omits credential options (including user) from GET responses.
            if k == "user" and actual is None:
                continue
            if actual != options[k]:
                raise RuntimeError(
                    "Existing lab connection points to different source; refusing to change it"
                )
    except NotFound:
        client.connections.create(
            connection,
            ConnectionType.POSTGRESQL,
            options,
            read_only=True,
            comment=f"LAB 10 {args.target}: fictional Wikipedia operations directory",
        )
    try:
        catalog = client.catalogs.get(foreign)
        if catalog.connection_name != connection:
            raise RuntimeError("Existing foreign catalog belongs to a different connection")
    except NotFound:
        client.catalogs.create(
            foreign,
            connection_name=connection,
            options={"database": env["LAB10_READER_DATABASE"]},
            comment=f"LAB 10 {args.target} Neon PostgreSQL; lab10 schema",
        )
    rows = sql_client(args.target)(f"SELECT count(*) AS projects FROM {foreign}.lab10.wiki_projects")
    print("Federation query:", rows)


if __name__ == "__main__":
    main()
