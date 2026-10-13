"""The job notebook keeps its SQL inline, so it must stay identical to the tested library SQL."""

import re
import runpy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from lab11 import processing
from lab11.common import identifier, render_sql

ROOT = Path(__file__).resolve().parents[1]
CATALOG, SCHEMA = "workspace", "ivanrazumovskyi_lab11_zerobus"
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_wiki_events"
SILVER = f"{CATALOG}.{SCHEMA}.silver_wiki_events"


def normalize(statement):
    """Ignore formatting only: whitespace, case and spacing around punctuation."""
    collapsed = re.sub(r"\s+", " ", statement.strip().rstrip(";")).lower()
    return re.sub(r"\s*([(),])\s*", r"\1", collapsed)


def notebook_statements():
    queries = []

    def sql(statement):
        queries.append(statement)
        return SimpleNamespace(collect=lambda: [[3]] if statement.startswith("SELECT COUNT(*)") else [])

    widgets = {"sql_root": str(ROOT / "src/lab11/sql"), "catalog": CATALOG, "schema": SCHEMA}
    runpy.run_path(
        str(ROOT / "notebooks/01_process.py"),
        init_globals={
            "dbutils": SimpleNamespace(widgets=SimpleNamespace(get=widgets.__getitem__)),
            "spark": SimpleNamespace(sql=sql),
            "display": Mock(),
        },
    )
    return queries


def test_notebook_conflict_check_matches_library():
    captured = []
    processing.assert_no_conflicts(lambda statement: captured.append(statement) or [], BRONZE)
    assert normalize(notebook_statements()[0]) == normalize(captured[0])


def test_notebook_merge_matches_library_template():
    expected = render_sql("01_merge.sql", bronze=BRONZE, silver=SILVER)
    assert normalize(notebook_statements()[1]) == normalize(expected)


def test_notebook_uses_quoted_identifiers_like_library():
    queries = notebook_statements()
    assert identifier(BRONZE, 3) in queries[0]
    assert identifier(SILVER, 3) in queries[1]


def test_comparison_detects_a_dropped_business_field():
    original = notebook_statements()[0]
    assert normalize(original) != normalize(original.replace("bytes_changed,", ""))
    assert normalize(original) == normalize(original.replace("\n", " ").upper())
