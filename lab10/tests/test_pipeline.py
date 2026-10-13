"""Exercise orchestration through its SQL boundary; live DEV checks verify SQL semantics."""

import pytest

from lab10.notebook_runner import run_notebook
from lab10.pipeline import Pipeline


class RecordingSQL:
    def __init__(self, latest=0, fail_reconciliation=False, drift=False):
        self.latest = latest
        self.checkpoint = 0
        self.fail_reconciliation = fail_reconciliation
        self.drift = drift
        self.statements = []

    def __call__(self, statement):
        sql = " ".join(statement.split())
        self.statements.append(sql)
        if sql.startswith("DESCRIBE DETAIL"):
            return [{"id": "source-uuid"}]
        if sql.startswith("DESCRIBE HISTORY"):
            return [{"version": self.latest}]
        if sql.startswith("SELECT * FROM") and sql.endswith("cdc_checkpoint"):
            return [
                {
                    "consumer": "wiki_projects",
                    "source_id": "source-uuid",
                    "last_processed_version": self.checkpoint,
                }
            ]
        if sql.startswith("UPDATE") and "cdc_checkpoint SET" in sql:
            self.checkpoint = int(sql.split("last_processed_version=", 1)[1].split()[0])
            return []
        if sql.startswith("SELECT count(*)"):
            count = 0
            if self.fail_reconciliation and "SELECT wiki_id, language_code" in sql and "EXCEPT ALL" in sql:
                count = 1
            if self.drift and "EXCEPT ALL" in sql:
                count = 2 if "FROM ivanrazumovskyi_lab10_neon_dev" in sql.split("EXCEPT ALL")[0] else 1
            if "fact_edits" in sql and "IS NULL" not in sql:
                count = 653
            return [{"count": count}]
        if sql.startswith("SELECT status,"):
            count = 42 if self.drift and "neon_dev" in sql else 43
            return [{"status": "active", "projects": count}]
        return []

    @property
    def writes(self):
        return [
            sql
            for sql in self.statements
            if sql.split()[0] in {"MERGE", "INSERT", "UPDATE", "DELETE", "CREATE"}
        ]


def test_no_new_changes_validates_without_writes():
    sql = RecordingSQL()
    assert Pipeline(sql).sync()["status"] == "NO_NEW_CHANGES"
    assert sql.writes == []
    assert any("EXCEPT ALL" in statement for statement in sql.statements)


def test_each_commit_is_consumed_and_checkpoint_is_last_write():
    sql = RecordingSQL(latest=2)
    p = Pipeline(sql)
    result = p.sync()
    assert result["status"] == "APPLIED"
    assert result["from_version"] == 1
    for table in (p.audit, p.current, p.history):
        merges = [statement for statement in sql.writes if statement.startswith(f"MERGE INTO {table} ")]
        assert len(merges) == 2
        assert p.changes(1, 1) in merges[0]
        assert p.changes(2, 2) in merges[1]
    assert sql.writes[-1].startswith(f"UPDATE {p.checkpoint} SET last_processed_version=2")
    assert "last_processed_version=0" in sql.writes[-1]
    checkpoint_index = sql.statements.index(sql.writes[-1])
    assert "EXCEPT ALL" in sql.statements[checkpoint_index - 1]
    assert sql.checkpoint == 2


@pytest.mark.parametrize("failure", ["partial_write", "reconciliation"])
def test_failure_never_publishes_checkpoint(failure):
    sql = RecordingSQL(latest=1, fail_reconciliation=failure == "reconciliation")
    p = Pipeline(sql)
    with pytest.raises((RuntimeError, AssertionError)):
        p.sync(fail_after_history_close=failure == "partial_write")
    assert sql.checkpoint == 0
    assert sql.writes
    assert not any(statement.startswith(f"UPDATE {p.checkpoint}") for statement in sql.writes)


def test_completed_simulation_has_no_writes():
    def sql(statement):
        assert statement.startswith("SELECT count(*)")
        return [{"count": 1}]

    assert Pipeline(sql).simulate("batch1")["status"] == "ALREADY_APPLIED"


def test_federation_reports_drift_and_warms_both_paths():
    sql = RecordingSQL(drift=True)
    result = run_notebook("01_federation", Pipeline(sql))
    assert result["snapshot_comparison"] == {
        "foreign_only_rows": 2,
        "snapshot_only_rows": 1,
        "matches_snapshot": False,
    }
    assert result["aggregates"]["foreign"] != result["aggregates"]["delta"]
    assert result["fact_rows"] == result["joined_rows"] == 653
    queries = [statement for statement in sql.statements if statement.startswith("SELECT status,")]
    assert len(queries) == 8  # two unmeasured warm-ups + six measured queries
    assert queries[:2] == queries[2:4] == queries[4:6] == queries[6:8]
    assert [item["path"] for item in result["timings"]] == ["foreign", "delta"] * 3
    assert sql.writes == []


def test_setup_notebook_preserves_an_existing_checkpoint():
    sql = RecordingSQL(latest=5)
    sql.checkpoint = 5

    def nonempty_source(statement):
        if statement.startswith("SELECT count(*) FROM workspace.ivanrazumovskyi_lab10.wiki_projects_source"):
            return [{"count": 55}]
        return sql(statement)

    result = run_notebook("00_setup", Pipeline(nonempty_source))
    assert result == {"baseline_projects": 55, "checkpoint": 5}
    checkpoint_merge = next(s for s in sql.writes if s.startswith("MERGE INTO") and "cdc_checkpoint" in s)
    assert "WHEN NOT MATCHED THEN INSERT" in checkpoint_merge
    assert "WHEN MATCHED" not in checkpoint_merge
    assert sql.checkpoint == 5


def test_validation_notebook_rejects_a_missing_insert():
    sql = RecordingSQL()
    with pytest.raises(AssertionError, match="Inserted demo project missing"):
        run_notebook("03_validate", Pipeline(sql))
    assert sql.writes == []
