# LAB 10 — Wikipedia Federation and CDC

Query an operational directory through Lakehouse Federation, enrich Wikipedia
facts, and maintain Delta current state and SCD Type 2 history from Delta CDF
using incremental `MERGE INTO`.

## Data and environments

Neon selection supplied by the owner:

| Resource | Name | ID |
|---|---|---|
| Organization | Ivan | `org-young-darkness-55090034` |
| Project | lab 10 | `blue-waterfall-78119366` |
| Branch | production | `br-royal-thunder-b4pcg6jy` |

Postgres connectivity, TLS and read-only Federation access have been verified
using the provided URL. Neon CLI account login is still absent, so ownership
has not been independently checked via the Neon management API. CLI 8.3.5 and
Neon skills are installed. Codex MCP uses OAuth pinned to this project;
MCP authentication remains pending. Neither is required for URL-based SQL work.

The user authorized filling the selected lab database. All PostgreSQL objects
are in `lab10`; existing objects elsewhere are untouched.

| Table | Data |
|---|---|
| `lab10.wiki_projects` | 55 observed Wikimedia projects: 43 personal LAB 9 + 12 additional Azure LAB 9 projects |
| `lab10.project_owners` | Five fictional teams with reserved `example.invalid` contacts |

Projects include Wikipedia, Wiktionary, Commons, Wikidata and other Wikimedia
projects. The original DEV export is `data/wiki_projects.json`; Azure additions
are in `data/wiki_projects_trial_additions.json`. Existing Neon rows are preserved.
DEV Delta sources retain their original 43-row baseline; new sources initialize
from all 55 rows. Team assignment,
status and support tier are synthetic and explicitly marked in the schema.
They do not describe actual Wikimedia responsibilities. No Netflix data is used.

| Resource | Personal DEV |
|---|---|
| Profile | `personal-env` |
| Connection | `ivanrazumovskyi_lab10_pg_dev` |
| Foreign catalog | `ivanrazumovskyi_lab10_neon_dev` |
| Delta schema | `workspace.ivanrazumovskyi_lab10` |
| Facts, read only | `workspace.dev_ivanrazumovskyi_ivanrazumovskyi_lab9_gold.fact_edits` |

The bundle includes an explicit `azure_trial_2` target for eventual promotion.
The user authorized promotion after DEV verification. The Azure bundle is deployed. The user created the connection and foreign
catalog through the UI, and both foreign tables are now queryable from trial_2.
Both Azure runs passed all three tasks: fresh CDC processing followed by a
whole-Job no-change rerun. Current/history/audit counts are 55/59/63, checkpoint 5.
See `VALIDATION.md` for the current status; separate PROD reader credentials
have been created for the same selected Neon database.

## Architecture

```text
Neon projects + owners -> UC read-only connection -> foreign catalog
    -> report drift against initial Delta snapshot
    -> LEFT JOIN existing Wikipedia facts on wiki = wiki_id
    -> seed dedicated Delta source once, with CDF enabled

Delta source -> bounded CDF range, consumed in commit order
    -> archive all events, including update preimages
    -> MERGE current state
    -> close/insert SCD2 versions
    -> reconcile source snapshot and targets -> advance checkpoint
```

Part B simulates changes in the dedicated **Delta source**. This is the Delta
CDF option permitted by the assignment, not PostgreSQL WAL capture or automatic
replication of future PostgreSQL changes. PostgreSQL reference rows remain
unchanged during the CDC simulation.

## Run

From the repository root:

```sh
python3 -m venv lab10/.venv
lab10/.venv/bin/python -m pip install -r lab10/requirements-dev.txt
```

Set `DATABASE_URL` in ignored `lab10/.env`. Optional `DATABASE_URL_UNPOOLED`
overrides it for schema work. Otherwise the seed derives the equivalent direct
Neon hostname in memory without changing supplied env values.

```sh
lab10/.venv/bin/python lab10/tools/export_projects.py
lab10/.venv/bin/python lab10/tools/seed_postgres.py
lab10/.venv/bin/python lab10/tools/setup_federation.py
lab10/.venv/bin/python lab10/tools/run_dev.py
```

Export is optional on reruns: the metadata snapshot is checked in. The seed is
transactional and additive (`ON CONFLICT DO NOTHING`), preserves existing rows,
and verifies reader access. It writes a generated credential to ignored
`.env.reader` with mode `0600`. Keep this file while keeping its reader role;
the seed refuses to reset a role whose credential is missing. Reader TLS checks
use the Linux system CA bundle.

Federation credentials are stored through the UC connection API, not placed in
SQL history, notebook code or Git. Local tooling defaults to `personal-env`. An explicit `--target azure_trial_2`
selects the Azure workspace; both targets enforce their pinned workspace host.

For the managed Job:

```sh
cd lab10
databricks bundle validate --strict -t personal --profile personal-env
databricks bundle deploy -t personal --profile personal-env
databricks bundle run lab10_job -t personal --profile personal-env
```

Setup, Federation and CDC tasks run sequentially on serverless notebook compute.
The Job has no schedule and allows one concurrent run. Serialize writers: do
not run the warehouse runner alongside the Job. Local `.env*`, virtual
environments and reports are excluded from upload.

## Demonstration and checks

1. Initialize source, current target, history, audit and checkpoint at version 0.
   Setup never resets an existing demo.
2. Report foreign/Delta row differences in both directions without failing on
   source drift. Warm both aggregate queries once, then interleave three measured
   runs per path; save timings/plans and verify the hybrid join cardinality.
3. Batch 1 inserts fictional `lab10_demo`, changes `enwiki` ownership/status,
   and deletes `plwiki` from the Delta source.
4. The warehouse runner injects a partial-write failure, proves the checkpoint
   did not move, and retries the same range.
5. Batch 2 changes `enwiki` twice before consumption. The second update has an
   older business timestamp, demonstrating the late-event policy.
6. Verify source/current equality at the frozen processed version, SCD2 current
   equality, unique event/version keys, non-overlapping history and complete audit.
   `enwiki` must retain four versions. A final rerun returns `NO_NEW_CHANGES`.

Current state uses inserts/update postimages and physically removes deletes.
SCD2 closes rows on updates/deletes and inserts new versions on inserts/updates.
Commit-by-commit processing retains intermediate history. The checkpoint pins
the source table ID so a recreated source cannot silently reuse an old offset.

```sh
lab10/.venv/bin/python -m ruff check lab10
lab10/.venv/bin/python -m ruff format --check lab10
lab10/.venv/bin/python -m pytest -c lab10/pyproject.toml lab10/tests
```

`reports/dev-e2e.json` stores live results and plans locally. CI runs only local
checks and never deploys either environment. See `VALIDATION.md` for verified runs.
Use `sql/02_review_dev.sql` for read-only SQL examples of the foreign catalog,
hybrid join, change events, history and checkpoint. The shared implementation
is `src/lab10/pipeline.py`; notebooks and the warehouse runner execute that same code.

## Discussion and limits

**Federation versus ingestion.** Federation reads operational data at query time
without maintaining another snapshot, but depends on source availability,
network latency and capacity. Supported operations can be pushed down. Delta
ingestion decouples analytics from source load at the cost of storage and
refresh logic. A snapshot becomes stale as the source changes. Our small dataset
is a functional demonstration: each path gets one unmeasured warm-up and measured
queries alternate foreign/Delta. Neon scale-to-zero adds cold-start latency;
client wall time and result caching still prevent treating this as a controlled
engine benchmark. Historical evidence collected before this revision had no
explicit warm-up and grouped the measurements by path.

Federation reports full-row differences (`EXCEPT ALL` in both directions) against
the frozen initial snapshot. Updates/inserts in Neon can legitimately change
this report and the aggregates; they do not refresh Delta or fail the comparison.
The hybrid join still fails if source edits remove required project/owner mappings
or multiply fact rows. Concurrent external edits are not a shared cross-system
snapshot, so compare reports from a quiet source when investigating drift.

**Security.** A dedicated Postgres reader has SELECT on the two lab tables;
the UC connection is read-only. Ingested copies also need UC access controls.
For PROD use separate credentials, namespaces and connections, approved network
access, and workspace bindings where the metastore is shared.

**Batch versus CDC.** Snapshots repeatedly move unchanged rows and omit
intermediate states. Recurring CDC batches consume only new CDF versions;
this is not a continuously running stream. CDF retention is finite, so consumed
events are archived. If a required range has expired, fail and explicitly rebuild
a baseline rather than silently skipping versions.

**SCD and late arrivals.** SCD1 exposes current state; SCD2 preserves versions.
History uses ingestion/commit order. `updated_at` is a business attribute and
can move backwards without discarding the arriving correction. This does not
retroactively repair business-effective intervals; that requires a reliable
source sequence, tie-breaking, a late-event policy and interval reconstruction.
If SCD2 used `updated_at` as effective time, the October 11 correction would have
to be inserted into the past and affected intervals split/recomputed. The
October 14 owner could remain current by business time, while our commit-order
policy makes the last received owner (3) current. A simple latest-timestamp MERGE
would discard the correction and lose that historical information.

**Recovery.** Current, audit, history and checkpoint writes are separate Delta
transactions. Readers can observe intermediate state during a failed run.
Serialize writers, use the checkpoint as the completed-batch marker, and retry
the unacknowledged range; event/version keys make writes replayable. Simulation
markers prevent repeating the demonstration on normal Job reruns.

## Promotion to Azure

The reproducible local provisioning path uses explicit targets:

```sh
lab10/.venv/bin/python lab10/tools/export_projects.py --target azure_trial_2
lab10/.venv/bin/python lab10/tools/seed_postgres.py --target azure_trial_2
lab10/.venv/bin/python lab10/tools/setup_federation.py --target azure_trial_2
```

The export preserves the DEV baseline file and writes only additional keys.
The additive seed preserves existing rows, creates `lab10_federation_reader_prod`
and saves its credential in ignored `.env.reader.prod` (mode 0600). The connection
API stores credentials in UC without putting them in SQL history. The user created the PostgreSQL connection through the UI; its credentials
were then changed to the dedicated reader via API. The user also created the foreign catalog through the UI from another workspace.
Both objects are accessible from trial_2, so metastore grants are no longer
required to run this Job. `sql/03_trial_admin_grants.sql` records the earlier
blocked API path; it was never executed. No permissions were escalated
by the tooling.

An alternative administrator-managed provisioning procedure follows:

1. Choose the PROD Neon endpoint/database and create a separate login with only
   `CONNECT` to that database, `USAGE ON SCHEMA lab10`, and `SELECT` on
   `lab10.wiki_projects` and `lab10.project_owners`. Populate/verify those two
   tables with the reference data; do not reuse the DEV owner's credentials.
2. Verify Azure compute can reach that endpoint on port 5432 and has Unity Catalog
   and serverless notebook support. The provisioning principal needs metastore
   `CREATE CONNECTION` and `CREATE CATALOG`. Confirm the configured run identity
   exists and can read `dbr_dev.ivanrazumovskyi_lab9_gold.fact_edits`; if the LAB 9
   location differs, explicitly override `gold_schema`/`catalog` in the target.
3. Create a restricted secret scope and enter values interactively (commands below
   use neither command-line passwords nor tracked env files). `host` is the direct
   Neon hostname, without a URL scheme; `database` is the database name.

```sh
databricks secrets create-scope lab10-prod --profile azure_trial_2
databricks secrets put-secret lab10-prod host --profile azure_trial_2
databricks secrets put-secret lab10-prod user --profile azure_trial_2
databricks secrets put-secret lab10-prod password --profile azure_trial_2
databricks secrets put-secret lab10-prod database --profile azure_trial_2
```

4. Run this one-time Python cell **inside the Azure workspace** as the provisioning
   principal. It uses managed secrets and creates a read-only UC connection. If
   names already exist, inspect them rather than replacing them blindly.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()
assert w.config.host.rstrip("/") == "https://adb-7405618361895520.0.azuredatabricks.net"
options = {key: dbutils.secrets.get("lab10-prod", key) for key in ("host", "user", "password")}
options["port"] = "5432"
w.connections.create(
    "ivanrazumovskyi_lab10_pg_catalog",
    ConnectionType.POSTGRESQL,
    options,
    read_only=True,
)
w.catalogs.create(
    "ivanrazumovskyi_lab10_neon_prod",
    connection_name="ivanrazumovskyi_lab10_pg_catalog",
    options={"database": dbutils.secrets.get("lab10-prod", "database")},
)
```

5. Give the Job run identity `USE CATALOG`, `USE SCHEMA`, and `SELECT` for the
   foreign tables and existing gold facts; give it `USE CATALOG` and
   `CREATE SCHEMA` on `dbr_dev` for its own LAB 10 schema. For an existing target
   schema, also grant `USE SCHEMA`, `CREATE TABLE`, `SELECT` and `MODIFY` there.
   Bind the foreign catalog
   to the intended workspace if the metastore is shared. The target sets
   `run_as` and `CAN_MANAGE` to `ivanrazumovskyi@softserve.academy`; confirm that
   identity and adjust it before deploying if necessary. The Job itself does
   not need access to the provisioning secret scope.
6. Query both foreign tables and verify 55 projects, 5 owners and required keys
   `enwiki`/`plwiki`. Then validate, deploy and run from `lab10/`:

```sh
databricks bundle validate --strict -t azure_trial_2 --profile azure_trial_2
databricks bundle deploy -t azure_trial_2 --profile azure_trial_2
databricks bundle run lab10_job -t azure_trial_2 --profile azure_trial_2
```

7. Check all three task outputs, CDC versions and final counts 55/59/63; repeat
   the Job and confirm `NO_NEW_CHANGES`. Save separate PROD evidence. DEV results
   do not establish Azure network access, permissions or runtime compatibility.

For a commit, stage `lab10/` and `.github/workflows/lab10-ci.yml` explicitly.
The root `skills-lock.json` belongs to local skill installation. Existing root
`.gitignore` changes and `lab9/VALIDATION.md` are outside this LAB 10 change.
Reviewable run evidence belongs under `lab10/data`; local full reports remain
ignored. `sync.exclude` is intentional: `sync.include` adds files to the default
sync set rather than making an exclusive upload allowlist.

## Sources

- [PostgreSQL Federation](https://docs.databricks.com/aws/en/query-federation/postgresql)
- [Change Data Feed](https://docs.databricks.com/aws/en/tables/features/change-data-feed)
- [Delta MERGE](https://docs.databricks.com/aws/en/delta/merge)
- [Neon pooling](https://neon.com/docs/connect/connection-pooling.md)
- [Bundle sync rules](https://docs.databricks.com/aws/en/dev-tools/bundles/reference#sync)
- [Azure Unity Catalog privileges](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/access-control/privileges-reference)
