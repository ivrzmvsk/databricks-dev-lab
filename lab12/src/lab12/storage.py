"""Shared row preparation, fingerprints and table-schema definitions."""

import json
from dataclasses import asdict
from operator import itemgetter

from lab12.corpus import digest


def quoted(name):
    return ".".join(f"`{part}`" for part in name.split("."))


def fingerprint(data, key):
    return digest(json.dumps(sorted(data, key=itemgetter(key)), sort_keys=True, ensure_ascii=False))


def snapshot_rows(documents, chunks):
    raw = [
        {
            "doc_id": d.doc_id,
            "name": d.name,
            "raw_text": d.text,
            "metadata_json": json.dumps(asdict(d), ensure_ascii=False),
            "content_hash": d.content_hash,
            "source_url": d.source_url,
            "snapshot": d.snapshot,
        }
        for d in documents
    ]
    if not raw or not chunks:
        raise ValueError("Refuse to persist an empty corpus")
    return [("documents", raw, "doc_id"), ("chunks", chunks, "chunk_id")]


def schema_ddl(rows):
    types = {str: "STRING", bool: "BOOLEAN", int: "BIGINT"}
    return ", ".join(f"`{key}` {types[type(value)]} NOT NULL" for key, value in rows[0].items())


def merge_snapshot(spark, full_table, frame, rows, key, view):
    """Replace a small existing table snapshot; never provision infrastructure.

    The caller supplies a frame with the shared explicit schema. Reject empty or
    duplicate snapshots before executing the MERGE that removes obsolete rows.
    """
    if not rows:
        raise ValueError("Refuse to persist an empty corpus")
    if len({r[key] for r in rows}) != len(rows):
        raise ValueError(f"Duplicate incoming keys in {full_table}")
    current = [r.asDict() for r in spark.table(full_table).collect()]
    if len({r[key] for r in current}) != len(current):
        raise ValueError(f"Duplicate target keys in {full_table}")
    expected_hash = fingerprint(rows, key)
    changed = fingerprint(current, key) != expected_hash
    if changed:
        frame.createOrReplaceTempView(view)
        try:
            spark.sql(
                f"MERGE INTO {quoted(full_table)} t USING {quoted(view)} s "
                f"ON t.`{key}` = s.`{key}` "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT * "
                "WHEN NOT MATCHED BY SOURCE THEN DELETE"
            )
        finally:
            spark.catalog.dropTempView(view)
        actual = [r.asDict() for r in spark.table(full_table).collect()]
        if fingerprint(actual, key) != expected_hash:
            raise ValueError(f"Snapshot reconciliation failed for {full_table}")
    return {"rows": len(rows), "changed": changed, "fingerprint": expected_hash}
