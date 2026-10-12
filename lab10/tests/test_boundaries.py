"""Protect write boundaries and the reference-data contract."""

import json
from pathlib import Path

import pytest

from lab10.pipeline import Config

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"schema": "default"},
        {"schema": "lab10; DROP SCHEMA default"},
        {"catalog": "workspace.other"},
        {"foreign_catalog": "source'"},
    ],
)
def test_reject_unsafe_or_shared_write_namespace(kwargs):
    with pytest.raises(ValueError):
        Config(**kwargs)


def test_reference_keys_and_cdc_examples_are_present():
    rows = json.loads((ROOT / "data/wiki_projects.json").read_text())
    keys = [row["wiki_id"] for row in rows]
    assert len(keys) == len(set(keys))
    assert {"enwiki", "plwiki", "ukwiki"}.issubset(keys)
    assert "lab10_demo" not in keys
    assert all(row["language_code"] and row["project_name"] for row in rows)


def test_only_supported_foreign_tables_can_be_addressed():
    with pytest.raises(ValueError):
        Config().foreign("arbitrary_table")
