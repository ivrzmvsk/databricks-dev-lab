# Lab 6 - Wikimedia Gold layer and business analytics

A business layer on top of the Lab 5 Wikipedia Silver table: a Gold star schema, an AI/BI dashboard with filters, a volume-drop alert with an email notification, and row-level and column-level security.
Everything is defined in a Databricks Asset Bundle and deployed with `databricks bundle deploy`.

Business question: **which Wikimedia communities are active, how much editing is automated, and which pages are edited most?**
The metrics describe the captured events, not all Wikimedia traffic.

```text
Lab 5 Silver (wiki_silver_lab5) + SiteMatrix snapshot
        -> Gold Job (refresh)
        -> 12 Delta tables in workspace.ivanrazumovskyi_lab6
        -> AI/BI dashboard, SQL alert, RLS/CLS
```

## What was built

- **Gold star schema:** one fact (`fact_edits`), five dimensions and three aggregates, plus three supporting tables (SiteMatrix reference, access mapping, volume log).
- **AI/BI dashboard** with KPIs, a five-minute trend, rankings and four global filters.
- **SQL alert** that fires when the Gold row count drops by 50% or more and sends an email. A reversible demo simulates the drop and the recovery.
- **Governance:** a row filter by wiki on eight tables, a column mask on editor names, and `SELECT` grants on the analytical tables only.
- **Four Jobs** that keep environment setup, security policies and data refresh apart, plus two demo Jobs.

Gold is built by a Job whose tasks are notebooks and is written as ordinary managed Delta tables. The tables are small and fully recomputed on each run, and row filters and masks can be attached to them directly. Lab 5 is only read, never changed.

## Jobs

| Job | What it does | When |
|---|---|---|
| `lab6-wikimedia-setup` | schema, metadata Volume, access mapping, volume history | once per environment |
| `lab6-wikimedia-governance` | policy functions, row filters, column mask, grants | after the first build and when access rules change |
| `lab6-wikimedia-gold` | references -> Gold -> validation -> volume record -> alert evaluation | every data refresh |
| `lab6-wikimedia-governance-demo` | temporarily restricts the user's access, checks enforcement, restores access | on demand |
| `lab6-wikimedia-volume-drop-demo` | deletes about 90% of the fact, triggers the alert, rebuilds Gold, validates and returns the alert to `OK` | on demand |

The refresh Job does not create the environment or reapply security: it only checks that the policies are still attached and fails otherwise.
A first-time environment is built in this order: setup, first Gold build (`bootstrap_mode=true`, which skips the policy check and the alert), governance, then a regular refresh.

Each task's code is in its own notebook, split into documented cells (`notebooks/`). The library (`src/lab6/`) keeps only what is shared or unit-tested: configuration, transformations and key generation, Wikimedia metadata parsing, table helpers and the read-only policy audit.
The volume demo recovers by running the same Build Gold, Validate and Volume notebooks as a normal refresh.

## Star schema

```text
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│ dim_date             │    │ dim_wiki             │    │ dim_namespace        │
│──────────────────────│    │──────────────────────│    │──────────────────────│
│ PK date_key          │    │ PK wiki_key          │    │ PK namespace_key     │
│ date                 │    │ wiki                 │    │ wiki                 │
│ year                 │    │ language_code        │    │ namespace            │
│ month                │    │ language_name        │    │ namespace_name       │
│ quarter              │    │ project_code         │    └──────────────────────┘
│ day_of_week          │    │ site_name            │
│ day_name             │    │ site_url             │
└──────────────────────┘    └──────────────────────┘
            │                           │                           │
            └───────────────────────────┬───────────────────────────┘        
                                        ▼
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│ dim_page             │    │ fact_edits           │    │ dim_editor           │
│──────────────────────│    │──────────────────────│    │──────────────────────│
│ PK page_key          │    │ event_id             │    │ PK editor_key        │
│ wiki                 │    │ event_time           │    │ wiki                 │
│ title                │◄───│ bucket_start         │───►│ user_name (masked)   │
│ first_seen           │    │ FK date_key          │    │ first_seen           │
│ last_seen            │    │ FK wiki_key          │    │ last_seen            │
└──────────────────────┘    │ FK namespace_key     │    └──────────────────────┘
                            │ FK page_key          │
                            │ FK editor_key        │
                            │ wiki (for RLS)       │
                            │ bot                  │
                            │ bytes_delta          │
                            │ bytes_added          │
                            │ bytes_removed        │
                            └──────────────────────┘
                                        │
            ┌───────────────────────────┬───────────────────────────┐        
            ▼                           ▼                           ▼
┌──────────────────────┐    ┌──────────────────────┐    ┌──────────────────────┐
│ agg_wiki_5min        │    │ agg_wiki_daily       │    │ agg_page_activity    │
│──────────────────────│    │──────────────────────│    │──────────────────────│
│ wiki_key             │    │ wiki_key             │    │ page_key             │
│ bucket_start         │    │ event_date           │    │ event_date           │
│ edit_count           │    │ edit_count           │    │ edit_count           │
│ editor_count         │    │ editor_count         │    │ editor_count         │
│ bot_share_pct        │    │ bot_share_pct        │    │ bot_share_pct        │
│ net_bytes_delta      │    │ net_bytes_delta      │    │ net_bytes_delta      │
└──────────────────────┘    └──────────────────────┘    └──────────────────────┘
```

Keys are SHA-256 surrogate keys scoped to the wiki (`dim_date` uses `yyyyMMdd`), so the same page title or user name on two wikis never collides. Replayed deliveries are deduplicated by `event_id`.
`bytes_delta = new_length - old_length` is a size change in bytes, not a quality score; unknown lengths stay NULL.

| Table | Grain |
|---|---|
| `fact_edits` | one unique `event_id` |
| `dim_wiki` | one observed wiki; language and project from SiteMatrix |
| `dim_date` | one UTC date |
| `dim_namespace` | one `(wiki, namespace_id)`, names from `siteinfo` |
| `dim_page` | one `(wiki, title)` |
| `dim_editor` | one `(wiki, user_name)`; the name is masked |
| `agg_wiki_5min` | wiki + UTC five-minute bucket |
| `agg_wiki_daily` | wiki + UTC date |
| `agg_page_activity` | page + UTC date |
| `ref_sitematrix_raw` | one SiteMatrix site (reference snapshot) |
| `sec_user_access` | user + allowed wiki (`*` = all) + permission to see editor names |
| `ops_volume_log` | one volume measurement per run, used by the alert |

`fact_edits` is built from one pinned Delta version of `wiki_silver_lab5`, recorded as the `lab6.source_version` table property, so validation compares against exactly the data that was used.
SiteMatrix is kept as a snapshot in a Unity Catalog volume (outbound network access is restricted in Free Edition); the Job can also refresh it from the public API.

## Governance

- `wiki_access` is a default-deny row filter on the eight tables that contain `wiki`. It reads `sec_user_access` and `session_user()`.
- `editor_mask` masks `dim_editor.user_name` as `[MASKED]` unless the mapping allows it.
- `reader_principal` gets `USE CATALOG`, `USE SCHEMA` and `SELECT` on the nine analytical tables only. The reference, access and ops tables are not granted.
- The personal workspace has one user, so enforcement is shown by temporarily changing that user's mapping, not with a second account.

## Dashboard

**Lab 6 - Wikimedia Activity** reads the Gold tables through four datasets and shows:

- four KPI counters: captured edits, observed editors, edited pages, net size change in bytes;
- a five-minute trend, edits by wiki, bot vs human edits, edits by namespace, top 20 pages and a daily summary;
- four global filters bound to all datasets: date, wiki, language and project.

Times are UTC. The top 20 pages are ranked on the whole capture; the filters narrow that list and do not recompute the ranking.
The dashboard uses the viewer's own data permissions, so row filters and masks apply to whoever opens it.

## Alert

The SQL alert compares the current `fact_edits` row count with the last healthy baseline from `ops_volume_log`. A drop of 50% or more triggers an email to the owner; recovery notifications are on.
It monitors the completeness of the Gold snapshot, not the live Wikimedia edit rate.

## Results

The capture contains 23,843 edits, 3,053 editors and about 19.6k pages.

Dashboard with four KPIs, five-minute trend, wiki ranking, bot share, namespaces, top pages and four global filters:

![dashboard](screenshots/01_dashboard.png)

Same dashboard filtered by `Language: de`:

![filtered dashboard](screenshots/02_dashboard_filtered.png)

Email from the volume-drop alert (`TRIGGERED`); the demo went from 23,843 to 2,389 rows and back to 23,843, with the alert returning to `OK`:

![alert email](screenshots/04_alert_email.png)

RLS and CLS check: 23,843 rows -> 7,826 rows for one wiki, 340 masked editors, 0 rows without a mapping:

![RLS and CLS](screenshots/05_rls_cls.png)

## Validation and tests

The `validation` task checks key uniqueness, foreign keys, reconciliation with the pinned Silver version, aggregate totals, and that the expected row filters and mask are attached.
Local Spark tests cover replay deduplication, wiki-scoped keys, UTC bucket boundaries, signed and missing byte changes, distinct-editor counts, reference parsing and the policy audit:

```bash
pip install -r requirements-dev.txt
python tests/test_transforms.py
python tests/test_security_metadata.py
```

## Project layout

| Path | What it is |
|---|---|
| `databricks.yml` | Bundle: variables and the `personal`, `azure_dev`, `azure_trial` targets |
| `resources/` | Setup, governance, refresh and demo Jobs, dashboard, alert |
| `notebooks/` | Task implementations, split into documented cells |
| `src/lab6/` | Shared code: configuration, transformations, metadata API, table helpers, policy audit |
| `dashboards/`, `tools/build_dashboard.py` | Dashboard JSON and the script that generates it |
| `assets/wikimedia.json` | SiteMatrix snapshot (fallback for restricted outbound network) |
| `tests/` | Local Spark tests for the transformations and the policy audit |
