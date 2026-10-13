"""Shared conflict gate and deterministic Bronze-to-Silver processing."""

from lab11.common import BRONZE, BUSINESS_COLUMNS, SILVER, identifier, render_sql


def assert_no_conflicts(execute, bronze=BRONZE):
    """Reject contradictory source content before mutating any Silver rows."""
    business = ",".join(c for c in BUSINESS_COLUMNS if c != "event_id")
    conflicts = execute(f"""SELECT event_id FROM {identifier(bronze, 3)} GROUP BY event_id
        HAVING event_id IS NULL OR COUNT(DISTINCT to_json(struct({business}))) > 1 LIMIT 1""")
    if conflicts:
        raise ValueError("Conflicting business payload for event_id")


def process(execute, bronze=BRONZE, silver=SILVER):
    assert_no_conflicts(execute, bronze)
    merge = render_sql("01_merge.sql", bronze=bronze, silver=silver)
    return execute(merge)
