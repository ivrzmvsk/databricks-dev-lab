# Databricks notebook source
# MAGIC %md
# MAGIC # LAB 10 — Lakehouse Federation
# MAGIC Read Neon in place, compare the ingested copy and enrich Delta facts.

# COMMAND ----------
import json

if "_injected_pipeline" in globals():
    pipeline = _injected_pipeline
else:
    import sys

    for name, value in {
        "code_root": "",
        "catalog": "workspace",
        "schema": "ivanrazumovskyi_lab10",
        "foreign_catalog": "ivanrazumovskyi_lab10_neon_dev",
        "gold_schema": "dev_ivanrazumovskyi_ivanrazumovskyi_lab9_gold",
    }.items():
        dbutils.widgets.text(name, value)
    sys.path.insert(0, dbutils.widgets.get("code_root") + "/src")
    from lab10.pipeline import Config, Pipeline

    config = Config(
        **{k: dbutils.widgets.get(k) for k in ("catalog", "schema", "foreign_catalog", "gold_schema")}
    )
    pipeline = Pipeline(lambda query: [r.asDict() for r in spark.sql(query).collect()], config)

if "spark" in globals():
    spark.conf.set("spark.sql.session.timeZone", "UTC")

import time

from lab10.pipeline import FIELDS

# COMMAND ----------
# MAGIC %md
# MAGIC ## Query PostgreSQL through the foreign catalog
# MAGIC Both EXCEPT ALL directions compare the live source with the frozen Delta copy.
# MAGIC Operational drift is reported without overwriting the snapshot.

# COMMAND ----------
external = pipeline.c.foreign("wiki_projects")
snapshot = pipeline.c.table("wiki_projects_snapshot")
# Drift is expected for an operational source; keep the frozen snapshot.
comparison = {}
for label, left, right in (
    ("foreign_only_rows", external, snapshot),
    ("snapshot_only_rows", snapshot, external),
):
    comparison[label] = pipeline.count(
        f"SELECT count(*) FROM ((SELECT {FIELDS} FROM {left}) EXCEPT ALL (SELECT {FIELDS} FROM {right}))"
    )
comparison["matches_snapshot"] = not any(comparison.values())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compare Federation and Delta query latency
# MAGIC Warm up each path once, then interleave three measured runs.
# MAGIC These small samples include client overhead and caching; Neon may sleep between runs.

# COMMAND ----------
queries = {
    label: f"SELECT status, count(*) AS projects FROM {table} WHERE status='active' GROUP BY status"
    for label, table in (("foreign", external), ("delta", snapshot))
}
# Exclude one warm-up per path; interleave measured paths to reduce order bias.
for query in queries.values():
    pipeline.sql(query)
measurements = []
aggregates = {}
for iteration in range(3):
    for label, query in queries.items():
        start = time.monotonic()
        rows = pipeline.sql(query)
        measurements.append(
            {
                "path": label,
                "iteration": iteration + 1,
                "elapsed_seconds": round(time.monotonic() - start, 3),
            }
        )
        aggregates[label] = rows

# COMMAND ----------
# MAGIC %md
# MAGIC ## Join external projects and owners with the Lab 9 Delta facts
# MAGIC Check lookup coverage and cardinality before showing the enriched result.

# COMMAND ----------
# LEFT JOIN retains all existing facts and proves the operational lookup covers every key.
fact = f"{pipeline.c.catalog}.{pipeline.c.gold_schema}.fact_edits"
join = f"""FROM {fact} f LEFT JOIN {external} p ON f.wiki=p.wiki_id
    LEFT JOIN {pipeline.c.foreign("project_owners")} o ON p.owner_id=o.owner_id"""
pipeline.assert_zero(
    f"SELECT count(*) {join} WHERE p.wiki_id IS NULL OR o.owner_id IS NULL", "Unmapped fact rows"
)
fact_count = pipeline.count(f"SELECT count(*) FROM {fact}")
joined_count = pipeline.count(f"SELECT count(*) {join}")
if not fact_count or joined_count != fact_count:
    raise AssertionError("Hybrid join must preserve every fact exactly once")
sample = pipeline.sql(f"""SELECT p.wiki_id, p.language_name, o.team_name, count(*) AS edits
    {join} GROUP BY p.wiki_id,p.language_name,o.team_name ORDER BY edits DESC LIMIT 10""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect the query plans
# MAGIC Federation can push supported filters into PostgreSQL. Delta queries use the local copy.

# COMMAND ----------
plans = {}
for label, table in (("foreign", external), ("delta", snapshot)):
    plans[label] = pipeline.sql(f"EXPLAIN FORMATTED SELECT wiki_id FROM {table} WHERE status='active'")
result = {
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
print(json.dumps(result, default=str, indent=2))
if "dbutils" in globals():
    dbutils.notebook.exit(json.dumps(result, default=str))
