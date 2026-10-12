"""SQL-only implementation shared by notebooks and the local warehouse runner.

The orchestrator must serialize all source simulations and consumers. Writes to
current, history, audit and checkpoint are separate transactions; recovery uses
idempotent event keys and publishes the checkpoint only after reconciliation.
"""

import re
import time
from dataclasses import dataclass

COLUMNS = (
    "wiki_id",
    "language_code",
    "language_name",
    "project_code",
    "project_name",
    "owner_id",
    "status",
    "support_tier",
    "updated_at",
    "ownership_is_synthetic",
)
FIELDS = ", ".join(COLUMNS)


@dataclass(frozen=True)
class Config:
    catalog: str = "workspace"
    schema: str = "ivanrazumovskyi_lab10"
    foreign_catalog: str = "ivanrazumovskyi_lab10_neon_dev"
    gold_schema: str = "dev_ivanrazumovskyi_ivanrazumovskyi_lab9_gold"

    def __post_init__(self):
        for value in (self.catalog, self.schema, self.foreign_catalog, self.gold_schema):
            if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", value):
                raise ValueError("Unsafe SQL identifier")
        if not self.schema.endswith("lab10"):
            raise ValueError("Writes must stay in the dedicated LAB 10 schema")

    def table(self, name):
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Unsafe table identifier")
        return f"{self.catalog}.{self.schema}.{name}"

    def foreign(self, name):
        if name not in ("wiki_projects", "project_owners"):
            raise ValueError("Unexpected external table")
        return f"{self.foreign_catalog}.lab10.{name}"


class Pipeline:
    def __init__(self, sql, config=None):
        self.sql = sql
        self.c = config or Config()
        self.source = self.c.table("wiki_projects_source")
        self.current = self.c.table("wiki_projects_current")
        self.history = self.c.table("wiki_projects_history")
        self.audit = self.c.table("wiki_change_events")
        self.checkpoint = self.c.table("cdc_checkpoint")

    def scalar(self, statement):
        rows = self.sql(statement)
        if len(rows) != 1 or len(rows[0]) != 1:
            raise RuntimeError("Expected exactly one SQL scalar")
        return next(iter(rows[0].values()))

    def count(self, statement):
        return int(self.scalar(statement))

    def assert_zero(self, statement, message):
        value = self.count(statement)
        if value:
            raise AssertionError(f"{message}: {value}")

    def equal(self, left, right, message):
        for a, b in ((left, right), (right, left)):
            self.assert_zero(f"SELECT count(*) FROM (({a}) EXCEPT ALL ({b}))", message)

    def detail(self):
        row = self.sql(f"DESCRIBE DETAIL {self.source}")[0]
        return row["id"]

    def latest(self):
        return int(self.sql(f"DESCRIBE HISTORY {self.source} LIMIT 1")[0]["version"])

    def setup(self):
        self.sql(f"CREATE SCHEMA IF NOT EXISTS {self.c.catalog}.{self.c.schema}")
        external = self.c.foreign("wiki_projects")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.source} USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
            AS SELECT {FIELDS} FROM {external}""")
        if not self.count(f"SELECT count(*) FROM {self.source}"):
            raise AssertionError("Refusing to initialize from empty external projects")
        # Version 0 remains the reproducible baseline even after simulations.
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.c.table("wiki_projects_snapshot")}
            USING DELTA AS SELECT {FIELDS} FROM {self.source} VERSION AS OF 0""")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.c.table("project_owners_snapshot")}
            USING DELTA AS SELECT * FROM {self.c.foreign("project_owners")}""")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.current} USING DELTA AS
            SELECT {FIELDS}, CAST(0 AS BIGINT) AS source_commit_version
            FROM {self.source} VERSION AS OF 0""")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.history} USING DELTA AS
            SELECT {FIELDS}, _commit_version AS source_commit_version,
              _commit_timestamp AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to,
              CAST(NULL AS BIGINT) AS valid_to_version, true AS is_current
            FROM table_changes('{self.source}', 0, 0) WHERE _change_type='insert'""")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.audit} USING DELTA AS
            SELECT * FROM table_changes('{self.source}', 0, 0)""")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.checkpoint}
            (consumer STRING, source_id STRING, last_processed_version BIGINT)
            USING DELTA""")
        self.sql(f"""MERGE INTO {self.checkpoint} t
            USING (SELECT 'wiki_projects' AS consumer, '{self.detail()}' AS source_id,
                          CAST(0 AS BIGINT) AS last_processed_version) s
            ON t.consumer=s.consumer
            WHEN NOT MATCHED THEN INSERT *""")
        self.sql(f"""CREATE TABLE IF NOT EXISTS {self.c.table("simulation_batches")}
            (batch STRING, completed_at TIMESTAMP) USING DELTA""")
        state = self.state()
        return {
            "baseline_projects": self.count(f"SELECT count(*) FROM {self.source} VERSION AS OF 0"),
            "checkpoint": state,
        }

    def state(self):
        rows = self.sql(f"SELECT * FROM {self.checkpoint}")
        if len(rows) != 1 or rows[0]["consumer"] != "wiki_projects":
            raise AssertionError("Expected one CDC consumer checkpoint")
        if rows[0]["source_id"] != self.detail():
            raise AssertionError("Source table was recreated; checkpoint cannot be reused")
        version = int(rows[0]["last_processed_version"])
        if version < 0 or version > self.latest():
            raise AssertionError("Invalid checkpoint version")
        return version

    def federation(self):
        external = self.c.foreign("wiki_projects")
        snapshot = self.c.table("wiki_projects_snapshot")
        # Drift is expected for an operational source; keep the frozen snapshot.
        comparison = {}
        for label, left, right in (
            ("foreign_only_rows", external, snapshot),
            ("snapshot_only_rows", snapshot, external),
        ):
            comparison[label] = self.count(
                f"SELECT count(*) FROM ((SELECT {FIELDS} FROM {left}) "
                f"EXCEPT ALL (SELECT {FIELDS} FROM {right}))"
            )
        comparison["matches_snapshot"] = not any(comparison.values())
        queries = {
            label: f"SELECT status, count(*) AS projects FROM {table} WHERE status='active' GROUP BY status"
            for label, table in (("foreign", external), ("delta", snapshot))
        }
        # Exclude one warm-up per path; interleave measured paths to reduce order bias.
        for query in queries.values():
            self.sql(query)
        measurements = []
        aggregates = {}
        for iteration in range(3):
            for label, query in queries.items():
                start = time.monotonic()
                rows = self.sql(query)
                measurements.append(
                    {
                        "path": label,
                        "iteration": iteration + 1,
                        "elapsed_seconds": round(time.monotonic() - start, 3),
                    }
                )
                aggregates[label] = rows
        # LEFT JOIN retains all existing facts and proves the operational lookup covers every key.
        fact = f"{self.c.catalog}.{self.c.gold_schema}.fact_edits"
        join = f"""FROM {fact} f LEFT JOIN {external} p ON f.wiki=p.wiki_id
            LEFT JOIN {self.c.foreign("project_owners")} o ON p.owner_id=o.owner_id"""
        self.assert_zero(
            f"SELECT count(*) {join} WHERE p.wiki_id IS NULL OR o.owner_id IS NULL", "Unmapped fact rows"
        )
        fact_count = self.count(f"SELECT count(*) FROM {fact}")
        joined_count = self.count(f"SELECT count(*) {join}")
        if not fact_count or joined_count != fact_count:
            raise AssertionError("Hybrid join must preserve every fact exactly once")
        sample = self.sql(f"""SELECT p.wiki_id, p.language_name, o.team_name, count(*) AS edits
            {join} GROUP BY p.wiki_id,p.language_name,o.team_name ORDER BY edits DESC LIMIT 10""")
        plans = {}
        for label, table in (("foreign", external), ("delta", snapshot)):
            plans[label] = self.sql(f"EXPLAIN FORMATTED SELECT wiki_id FROM {table} WHERE status='active'")
        return {
            "snapshot_comparison": comparison,
            "aggregates": aggregates,
            "fact_rows": fact_count,
            "joined_rows": joined_count,
            "sample": sample,
            "timings": measurements,
            "plans": plans,
            "timing_note": "One warm-up per path excluded; paths interleaved. Client wall time includes "
            "network/polling and result caches; Neon scale-to-zero can affect cold queries. "
            "Aggregates may differ when the operational source has changed.",
        }

    def changes(self, start, end):
        return f"table_changes('{self.source}', {int(start)}, {int(end)})"

    def validate(self, version=None):
        end = self.state() if version is None else version
        source = f"SELECT {FIELDS} FROM {self.source} VERSION AS OF {int(end)}"
        self.assert_zero(f"SELECT count(*) FROM ({source}) WHERE wiki_id IS NULL", "Null source key")
        for table, key in (
            (self.current, "wiki_id"),
            (self.history, "wiki_id,source_commit_version"),
            (self.audit, "wiki_id,_commit_version,_change_type"),
        ):
            self.assert_zero(
                f"SELECT count(*) FROM (SELECT {key} FROM {table} GROUP BY {key} HAVING count(*)>1)",
                f"Duplicate keys in {table}",
            )
        self.equal(source, f"SELECT {FIELDS} FROM {self.current}", "Current target/source mismatch")
        self.equal(
            source, f"SELECT {FIELDS} FROM {self.history} WHERE is_current", "SCD2 current/source mismatch"
        )
        self.assert_zero(
            f"""SELECT count(*) FROM {self.history}
            WHERE (is_current AND (valid_to IS NOT NULL OR valid_to_version IS NOT NULL))
               OR (NOT is_current AND (valid_to IS NULL OR valid_to_version IS NULL
                                       OR valid_to_version<=source_commit_version))""",
            "Invalid SCD2 interval",
        )
        self.assert_zero(
            f"""SELECT count(*) FROM (
            SELECT *, lead(source_commit_version) OVER (PARTITION BY wiki_id ORDER BY source_commit_version) next_version
            FROM {self.history})
            WHERE next_version IS NOT NULL AND (valid_to_version IS NULL OR valid_to_version>next_version)""",
            "Overlapping SCD2 intervals",
        )
        return {
            "version": end,
            "current_rows": self.count(f"SELECT count(*) FROM {self.current}"),
            "history_rows": self.count(f"SELECT count(*) FROM {self.history}"),
            "audit_rows": self.count(f"SELECT count(*) FROM {self.audit}"),
        }

    def sync(self, fail_after_history_close=False):
        start = self.state()
        end = self.latest()  # freeze the inclusive upper bound for this batch
        if end == start:
            return {"status": "NO_NEW_CHANGES", **self.validate(end)}
        self.assert_zero(
            f"""SELECT count(*) FROM (SELECT wiki_id
            FROM {self.source} VERSION AS OF {end} GROUP BY wiki_id
            HAVING wiki_id IS NULL OR count(*)>1)""",
            "Invalid source keys",
        )
        # Consume commits in order. Collapsing the whole batch would lose intermediate SCD2 versions.
        for version in range(start + 1, end + 1):
            changes = self.changes(version, version)
            self.assert_zero(f"SELECT count(*) FROM {changes} WHERE wiki_id IS NULL", "Null change key")
            self.assert_zero(
                f"""SELECT count(*) FROM (SELECT wiki_id,_change_type FROM {changes}
                GROUP BY wiki_id,_change_type HAVING count(*)>1)""",
                "Ambiguous change events",
            )
            self.sql(f"""MERGE INTO {self.audit} t USING (SELECT * FROM {changes}) s
                ON t.wiki_id=s.wiki_id AND t._commit_version=s._commit_version AND t._change_type=s._change_type
                WHEN NOT MATCHED THEN INSERT *""")
            final = f"SELECT * FROM {changes} WHERE _change_type IN ('insert','update_postimage','delete')"
            self.assert_zero(
                f"SELECT count(*) FROM (SELECT wiki_id FROM ({final}) GROUP BY wiki_id HAVING count(*)>1)",
                "Multiple final events per key in one commit",
            )
            assignments = ", ".join(f"t.{col}=s.{col}" for col in COLUMNS)
            insert_columns = FIELDS + ", source_commit_version"
            insert_values = ", ".join(f"s.{col}" for col in COLUMNS) + ", s._commit_version"
            self.sql(f"""MERGE INTO {self.current} t USING ({final}) s ON t.wiki_id=s.wiki_id
                WHEN MATCHED AND s._change_type='delete' AND t.source_commit_version<=s._commit_version THEN DELETE
                WHEN MATCHED AND s._change_type<>'delete' AND t.source_commit_version<s._commit_version
                  THEN UPDATE SET {assignments}, t.source_commit_version=s._commit_version
                WHEN NOT MATCHED AND s._change_type<>'delete'
                  THEN INSERT ({insert_columns}) VALUES ({insert_values})""")
            self.sql(f"""MERGE INTO {self.history} t USING ({final}) s
                ON t.wiki_id=s.wiki_id AND t.is_current AND t.source_commit_version<s._commit_version
                WHEN MATCHED THEN UPDATE SET t.is_current=false, t.valid_to=s._commit_timestamp,
                                             t.valid_to_version=s._commit_version""")
            if fail_after_history_close:
                raise RuntimeError("Injected failure after SCD2 close; checkpoint intentionally unchanged")
            history_values = ", ".join(f"s.{col}" for col in COLUMNS)
            self.sql(f"""INSERT INTO {self.history}
                SELECT {history_values}, s._commit_version, s._commit_timestamp,
                       CAST(NULL AS TIMESTAMP), CAST(NULL AS BIGINT), true
                FROM ({final}) s
                WHERE s._change_type<>'delete' AND NOT EXISTS (
                    SELECT 1 FROM {self.history} h WHERE h.wiki_id=s.wiki_id
                      AND h.source_commit_version=s._commit_version)""")
        result = self.validate(end)
        self.equal(
            f"SELECT * FROM {self.changes(start + 1, end)}",
            f"SELECT * FROM {self.audit} WHERE _commit_version BETWEEN {start + 1} AND {end}",
            "Incomplete batch audit",
        )
        # The checkpoint is deliberately the last mutation after all reconciliation.
        self.sql(f"""UPDATE {self.checkpoint} SET last_processed_version={end}
            WHERE consumer='wiki_projects' AND last_processed_version={start}""")
        if self.state() != end:
            raise AssertionError("Checkpoint conflict: serialize writers")
        return {"status": "APPLIED", "from_version": start + 1, **result}

    def simulate(self, batch):
        if batch not in ("batch1", "batch2"):
            raise ValueError("Unknown simulation batch")
        marker = self.c.table("simulation_batches")
        if self.count(f"SELECT count(*) FROM {marker} WHERE batch='{batch}'"):
            return {"batch": batch, "status": "ALREADY_APPLIED"}
        if batch == "batch1":
            for key in ("enwiki", "plwiki"):
                if (
                    self.count(f"SELECT count(*) FROM {self.source} VERSION AS OF 0 WHERE wiki_id='{key}'")
                    != 1
                ):
                    raise AssertionError("Simulation needs seeded enwiki and plwiki")
            self.sql(f"""MERGE INTO {self.source} t USING (SELECT
                'lab10_demo' wiki_id, 'und' language_code, 'Synthetic' language_name,
                'demo' project_code, 'LAB 10 fictional project' project_name, 4 owner_id,
                'active' status, 'standard' support_tier,
                TIMESTAMP '2026-10-13 00:00:00' updated_at, true ownership_is_synthetic) s
                ON t.wiki_id=s.wiki_id WHEN NOT MATCHED THEN INSERT *""")
            self.sql(f"""UPDATE {self.source} SET owner_id=5, status='maintenance',
                updated_at=TIMESTAMP '2026-10-13 00:00:00'
                WHERE wiki_id='enwiki' AND NOT (owner_id=5 AND status='maintenance'
                    AND updated_at=TIMESTAMP '2026-10-13 00:00:00')""")
            self.sql(f"DELETE FROM {self.source} WHERE wiki_id='plwiki'")
        else:
            if not self.count(f"SELECT count(*) FROM {marker} WHERE batch='batch1'"):
                raise AssertionError("Run batch1 before batch2")
            # Multiple commits for one key before sync: all versions must reach SCD2.
            self.sql(f"""UPDATE {self.source} SET owner_id=2, status='active',
                updated_at=TIMESTAMP '2026-10-14 00:00:00'
                WHERE wiki_id='enwiki' AND owner_id=5""")
            # Late business timestamp; policy is ingestion/commit-order history.
            self.sql(f"""UPDATE {self.source} SET owner_id=3,
                updated_at=TIMESTAMP '2026-10-11 00:00:00'
                WHERE wiki_id='enwiki' AND owner_id=2""")
        self.sql(f"""MERGE INTO {marker} t USING (SELECT '{batch}' batch, current_timestamp() completed_at) s
            ON t.batch=s.batch WHEN NOT MATCHED THEN INSERT *""")
        return {"batch": batch, "status": "APPLIED"}

    def assert_scenario(self):
        if self.count(f"SELECT count(*) FROM {self.current} WHERE wiki_id='plwiki'"):
            raise AssertionError("Deleted project remains in target")
        if self.count(f"SELECT count(*) FROM {self.current} WHERE wiki_id='lab10_demo'") != 1:
            raise AssertionError("Inserted demo project missing or duplicated")
        if self.count(f"SELECT count(*) FROM {self.current} WHERE wiki_id='enwiki' AND owner_id=3") != 1:
            raise AssertionError("Late committed update was not applied")
        if self.count(f"SELECT count(*) FROM {self.history} WHERE wiki_id='enwiki'") != 4:
            raise AssertionError("Intermediate SCD2 updates were lost")
        self.equal(
            f"SELECT * FROM {self.changes(0, self.state())}",
            f"SELECT * FROM {self.audit}",
            "Full demonstration audit mismatch",
        )
        return self.validate()
