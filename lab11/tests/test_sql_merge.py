"""Execute the actual packaged MERGE on DuckDB (only identifier quotes adapted)."""

import duckdb
import pytest

from lab11.common import BRONZE, SILVER, render_sql


@pytest.fixture
def database():
    db = duckdb.connect()
    db.execute("ATTACH ':memory:' AS workspace")
    db.execute("CREATE SCHEMA workspace.ivanrazumovskyi_lab11_zerobus")
    db.execute(f"""CREATE TABLE {BRONZE} (
        event_id VARCHAR, wiki VARCHAR, page_title VARCHAR, user_name VARCHAR,
        event_type VARCHAR, bytes_changed BIGINT, is_bot BOOLEAN, sequence_number BIGINT,
        event_timestamp TIMESTAMP, producer_id VARCHAR, run_id VARCHAR,
        batch_id VARCHAR, produced_at TIMESTAMP)""")
    db.execute(f"""CREATE TABLE {SILVER} AS SELECT event_id, wiki, page_title, user_name,
        event_type, bytes_changed, is_bot, sequence_number, event_timestamp, producer_id, run_id,
        batch_id AS first_batch_id, produced_at AS first_produced_at, 0::BIGINT AS delivery_count
        FROM {BRONZE}""")
    yield db
    db.close()


def insert(db, event_id="source-1", batch="batch-1", timestamp="2026-10-13 10:00:00"):
    db.execute(
        f"""INSERT INTO {BRONZE} VALUES (?, 'enwiki', 'Example', 'Editor', 'edit',
        50, false, 42, TIMESTAMP '2026-10-13 09:00:00', 'producer', 'run', ?, ?)""",
        [event_id, batch, timestamp],
    )


def merge(db):
    db.execute(render_sql("01_merge.sql").replace("`", '"'))


def snapshot(db):
    return db.execute(f"SELECT * FROM {SILVER} ORDER BY event_id").fetchall()


def test_merge_deduplicates_and_repeated_processing_is_unchanged(database):
    insert(database)
    insert(database, batch="batch-2", timestamp="2026-10-13 11:00:00")
    insert(database, event_id="source-2")
    merge(database)
    before = snapshot(database)
    assert len(before) == 2
    assert before[0][-3] == "batch-1"
    assert [r[-1] for r in before] == [2, 1]
    merge(database)
    assert snapshot(database) == before


def test_late_duplicate_updates_only_delivery_count(database):
    insert(database)
    merge(database)
    before = snapshot(database)[0]
    insert(database, batch="late", timestamp="2026-10-13 12:00:00")
    merge(database)
    after = snapshot(database)
    assert len(after) == 1
    assert after[0][:-1] == before[:-1]
    assert after[0][-1] == 2


@pytest.mark.parametrize("column", ["first_batch_id", "first_produced_at", "delivery_count"])
def test_merge_repairs_null_metadata(database, column):
    insert(database)
    merge(database)
    expected = snapshot(database)
    database.execute(f"UPDATE {SILVER} SET {column} = NULL")
    merge(database)
    assert snapshot(database) == expected


def test_earlier_delivery_updates_first_metadata(database):
    insert(database, batch="later", timestamp="2026-10-13 12:00:00")
    merge(database)
    insert(database, batch="earlier", timestamp="2026-10-13 10:00:00")
    merge(database)
    assert snapshot(database)[0][-3] == "earlier"
