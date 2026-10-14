from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lab12.corpus import prepare
from lab12.storage import fingerprint, merge_snapshot, schema_ddl, snapshot_rows


def test_setup_schema_and_ingestion_rows_share_the_same_contract():
    documents, chunks = prepare(Path(__file__).resolve().parents[1] / "dataset/board_games.json")
    batches = snapshot_rows(documents, chunks)
    assert [(table, len(rows), key) for table, rows, key in batches] == [
        ("documents", 12, "doc_id"),
        ("chunks", 12, "chunk_id"),
    ]
    raw = batches[0][1]
    assert raw[0]["raw_text"] == documents[0].text
    ddl = schema_ddl(chunks)
    assert "`coop` BOOLEAN NOT NULL" in ddl
    assert "`minutes` BIGINT NOT NULL" in ddl
    assert "`chunk_text` STRING NOT NULL" in ddl
    assert fingerprint(raw, "doc_id") == fingerprint(list(reversed(raw)), "doc_id")


def spark_rows(rows):
    result = []
    for row in rows:
        item = MagicMock()
        item.asDict.return_value = row
        result.append(item)
    return result


def test_merge_changed_snapshot_and_cleanup():
    spark, frame = MagicMock(), MagicMock()
    incoming = [{"id": "changed", "value": 2}, {"id": "new", "value": 3}]
    old = [{"id": "changed", "value": 1}, {"id": "obsolete", "value": 0}]
    spark.table.return_value.collect.side_effect = [spark_rows(old), spark_rows(incoming)]
    result = merge_snapshot(spark, "c.s.t", frame, incoming, "id", "incoming")
    assert result == {"rows": 2, "changed": True, "fingerprint": fingerprint(incoming, "id")}
    statement = spark.sql.call_args.args[0]
    assert "WHEN MATCHED THEN UPDATE SET *" in statement
    assert "WHEN NOT MATCHED THEN INSERT *" in statement
    assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in statement
    spark.catalog.dropTempView.assert_called_once_with("incoming")


def test_merge_identical_snapshot_never_writes():
    spark, frame = MagicMock(), MagicMock()
    rows = [{"id": "a"}, {"id": "b"}]
    spark.table.return_value.collect.return_value = spark_rows(list(reversed(rows)))
    assert merge_snapshot(spark, "c.s.t", frame, rows, "id", "incoming")["changed"] is False
    spark.sql.assert_not_called()
    frame.createOrReplaceTempView.assert_not_called()


@pytest.mark.parametrize(
    "incoming,current,message",
    [
        ([], [], "empty"),
        ([{"id": "a"}] * 2, [], "incoming keys"),
        ([{"id": "a"}], [{"id": "a"}] * 2, "target keys"),
    ],
)
def test_merge_rejects_unsafe_snapshot_before_writing(incoming, current, message):
    spark, frame = MagicMock(), MagicMock()
    spark.table.return_value.collect.return_value = spark_rows(current)
    with pytest.raises(ValueError, match=message):
        merge_snapshot(spark, "c.s.t", frame, incoming, "id", "incoming")
    spark.sql.assert_not_called()
    frame.createOrReplaceTempView.assert_not_called()


def test_merge_failure_cleans_up():
    spark, frame = MagicMock(), MagicMock()
    spark.table.return_value.collect.return_value = []
    spark.sql.side_effect = RuntimeError("write failed")
    with pytest.raises(RuntimeError, match="write failed"):
        merge_snapshot(spark, "c.s.t", frame, [{"id": "a"}], "id", "incoming")
    spark.catalog.dropTempView.assert_called_once_with("incoming")


def test_merge_reconciliation_rejects_incomplete_write():
    spark, frame = MagicMock(), MagicMock()
    spark.table.return_value.collect.return_value = []
    with pytest.raises(ValueError, match="reconciliation"):
        merge_snapshot(spark, "c.s.t", frame, [{"id": "a"}], "id", "incoming")
    spark.catalog.dropTempView.assert_called_once_with("incoming")
