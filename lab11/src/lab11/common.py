"""Configuration, SQL templates and bounded warehouse execution."""

import json
import os
import re
import time
from pathlib import Path
from string import Template

PACKAGE = Path(__file__).resolve().parent
ROOT = Path(os.environ.get("LAB11_ROOT", PACKAGE.parents[1]))
BUSINESS_COLUMNS = (
    "event_id",
    "wiki",
    "page_title",
    "user_name",
    "event_type",
    "bytes_changed",
    "is_bot",
    "sequence_number",
    "event_timestamp",
)
TARGETS = json.loads((PACKAGE / "targets.json").read_text())
DEV = TARGETS["personal"]
HOST, ENDPOINT, SCHEMA, WAREHOUSE = (DEV[k] for k in ("host", "endpoint", "schema", "warehouse"))
BRONZE = f"{SCHEMA}.bronze_wiki_events"
SILVER = f"{SCHEMA}.silver_wiki_events"


def identifier(value, parts):
    tokens = value.split(".")
    if len(tokens) != parts or not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", t) for t in tokens):
        raise ValueError(f"Expected a {parts}-part SQL identifier")
    return ".".join(f"`{token}`" for token in tokens)


def render_sql(name, *, bronze=BRONZE, silver=SILVER, schema=SCHEMA):
    if name not in ("00_create_tables.sql", "01_merge.sql"):
        raise ValueError("Unknown SQL template")
    return Template((PACKAGE / "sql" / name).read_text()).substitute(
        bronze=identifier(bronze, 3), silver=identifier(silver, 3), schema=identifier(schema, 2)
    )


def statements(text):
    import sqlparse

    return [
        s
        for s in sqlparse.split(text, strip_semicolon=True)
        if sqlparse.format(s, strip_comments=True).strip()
    ]


def credentials(target="personal"):
    from dotenv import dotenv_values

    if target not in TARGETS:
        raise ValueError("Unknown target")
    values = dotenv_values(ROOT / f".env.{target}", interpolate=False)
    keys = ("ZEROBUS_CLIENT_ID", "ZEROBUS_CLIENT_SECRET")
    if not all(values.get(k) for k in keys):
        raise ValueError(f"Missing Zerobus credentials in .env.{target}")
    return tuple(values[k] for k in keys)


def dev_client():
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=DEV["profile"])
    if client.config.host.rstrip("/") != HOST:
        raise ValueError("personal-env must resolve to the pinned DEV host")
    return client


def sql(client, statement, *, warehouse=WAREHOUSE):
    api = client.statement_execution
    result = api.execute_statement(warehouse_id=warehouse, statement=statement, wait_timeout="10s")
    deadline = time.monotonic() + 300
    while result.status and result.status.state.value in ("PENDING", "RUNNING"):
        if time.monotonic() > deadline:
            api.cancel_execution(result.statement_id)
            raise TimeoutError(f"SQL statement {result.statement_id} exceeded 300 seconds")
        time.sleep(2)
        result = api.get_statement(result.statement_id)
    state = result.status.state.value if result.status and result.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        error = result.status.error if result.status else None
        message = error.message if error else "No error details returned by server"
        raise RuntimeError(f"SQL {result.statement_id} ended with {state}: {message}")
    if result.manifest and result.manifest.truncated:
        raise RuntimeError(f"SQL {result.statement_id} result truncated; narrow the query")
    chunk = result.result
    rows = []
    while chunk is not None:
        rows.extend(chunk.data_array or [])
        if chunk.next_chunk_index is None:
            break
        chunk = api.get_statement_result_chunk_n(result.statement_id, chunk.next_chunk_index)
    return rows


def save_report(name, report):
    directory = ROOT / ".state"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(json.dumps(report, indent=2) + "\n")


def load_manifest(path=None):
    source = Path(path) if path else PACKAGE / "fixtures/wikimedia_sample.json"
    manifest = json.loads(source.read_text())
    if not manifest.get("run_id") or not manifest.get("records"):
        raise ValueError("Manifest needs a run_id and records")
    records = manifest["records"]
    if any(not r.get("event_id") or any(k not in r for k in BUSINESS_COLUMNS) for r in records):
        raise ValueError("Manifest record is missing business fields or event ID")
    if len({r["event_id"] for r in records}) != len(records):
        raise ValueError("Manifest event IDs must be unique")
    return manifest
