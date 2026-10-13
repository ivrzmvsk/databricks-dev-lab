"""Explicit environment for live acceptance checks; defaults to personal DEV."""

import time
from functools import cached_property

from lab11 import common
from lab11.processing import process


class AcceptanceEnvironment:
    def __init__(self, target="personal"):
        self.target = target
        self.config = dict(common.TARGETS[target])
        self.schema = self.config["schema"]
        self.bronze = self.schema + ".bronze_wiki_events"
        self.silver = self.schema + ".silver_wiki_events"
        self.host = self.config.get("ingest_host", self.config["host"])
        self.endpoint = self.config.get("ingest_endpoint", self.config["endpoint"])
        if self.host != self.config["host"] or self.endpoint != self.config["endpoint"]:
            raise ValueError("Acceptance checks require ingestion and compute in the same workspace")

    @cached_property
    def client(self):
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient(profile=self.config["profile"])
        if client.config.host.rstrip("/") != self.config["host"]:
            raise ValueError("Profile does not match the selected target's pinned host")
        return client

    def execute(self, statement):
        return common.sql(self.client, statement, warehouse=self.config["warehouse"])

    def wait_rows(self, table, minimum):
        deadline = time.monotonic() + 120
        while True:
            count = int(self.execute(f"SELECT COUNT(*) FROM {table}")[0][0])
            if count >= minimum:
                return count
            if time.monotonic() > deadline:
                raise TimeoutError("Rows not queryable")
            time.sleep(2)

    def process(self, bronze=None, silver=None):
        bronze, silver = bronze or self.bronze, silver or self.silver
        for name in (bronze, silver):
            common.identifier(name, 3)
            if not name.startswith(self.schema + "."):
                raise ValueError("Processing outside Lab 11 schema is forbidden")
        return process(self.execute, bronze, silver)

    def credentials(self):
        return common.credentials(self.target)

    def send(self, records, run_id, **kwargs):
        from lab11.producer import send

        return send(records, run_id, target=self.target, **kwargs)

    def save_report(self, name, data):
        prefix = "trial-" if self.target == "azure_trial_2" else ""
        common.save_report(
            prefix + name,
            dict(data, target=self.target, compute_host=self.config["host"], ingest_host=self.host),
        )
