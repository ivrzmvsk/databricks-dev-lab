"""Explicit environment bindings; no default credentials or arbitrary table names."""

import re
from dataclasses import dataclass

DEV_HOST = "https://dbc-ab9a5afe-c151.cloud.databricks.com"
PROD_HOST = "https://adb-7405618361895520.0.azuredatabricks.net"


@dataclass(frozen=True)
class Settings:
    target: str = "dev"
    catalog: str = "workspace"
    schema: str = "ivanrazumovskyi_lab12_steam"
    vector_endpoint: str = "ivanrazumovskyi-lab12-steam-dev"
    embedding_model: str = "databricks-qwen3-embedding-0-6b"
    chat_model: str = "databricks-qwen3-next-80b-a3b-instruct"
    top_k: int = 3
    max_context_tokens: int = 6000
    max_answer_tokens: int = 1200

    def __post_init__(self):
        if self.target not in ("dev", "prod"):
            raise ValueError("Unknown target")
        if self.catalog != ("workspace" if self.target == "dev" else "dbr_dev"):
            raise ValueError("Catalog does not match target")
        expected_schema = {
            "dev": "ivanrazumovskyi_lab12_steam",
            "prod": "ivanrazumovskyi_lab12_board_games_prod",
        }[self.target]
        if self.schema != expected_schema:
            raise ValueError("Schema must belong to Lab 12 and match target")
        if not 1 <= self.top_k <= 20 or self.max_context_tokens < 400:
            raise ValueError("Invalid retrieval/context budget")

    @classmethod
    def from_widgets(cls, dbutils):
        keys = ("target", "catalog", "schema", "vector_endpoint", "embedding_model", "chat_model")
        return cls(**{key: dbutils.widgets.get(key) for key in keys})

    @property
    def host(self):
        return DEV_HOST if self.target == "dev" else PROD_HOST

    def assert_host(self, host):
        if host.rstrip("/") != self.host:
            raise ValueError("Workspace host does not match explicit target")

    def table(self, name):
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
            raise ValueError("Invalid table identifier")
        name = {
            "documents": "board_documents",
            "chunks": "board_chunks",
            "chunks_index": "board_chunks_index",
        }.get(name, name)
        return f"{self.catalog}.{self.schema}.{name}"
