# Lab 8 - CI/CD: DEV to PROD promotion

The Wikipedia pipeline and Gold analytics from Labs 5-7, packaged as one Databricks Asset Bundle with two environments and a GitHub Actions workflow.
Pull requests run tests and validate DEV. A merge to `main` deploys PROD, runs the promotion Job and waits for its result.

Lab 5 parsing and quality rules and Lab 6 transformations are reused from their shared modules. Lab 8 creates its own schemas and resources. The Wikipedia producer runs on an external machine and sends events to the existing Event Hub.

## Flow

```mermaid
flowchart TD
    PR[Pull request to main] --> T[Local Spark tests + offline bundle checks]
    T --> V[Bundle validate: personal DEV]
    V --> M[Merge to main]
    M --> C[Tests + DEV validation on merged commit]
    C --> P[Bundle validate + deploy: Azure PROD]
    P --> J[Run promotion_job and wait for its release gate]
```

PROD is deployed only by a push to `main`. Pull requests and manual runs only check.

## What was built

- **One bundle** `lab8_wikipedia_promotion`, with `dev` in the personal workspace and `prod` in Azure trial 2. Both use serverless compute.
- **One Lakeflow pipeline:** Event Hub -> Wikipedia Bronze -> Silver and quarantine, with named expectations and preserved Kafka coordinates.
- **One eight-task Job:** setup, ingestion, Gold analytics, governance, validation and release checks.
- **Everything as code:** schemas, pipeline, Job, SQL migrations, metadata Volume, dashboard and its four queries, row filters, column mask and permissions.

The Gold model and dashboard follow Lab 6: one edit fact, five dimensions and three aggregates. The dashboard uses the viewer's own data permissions (`embed_credentials: false`). Azure infrastructure is pre-existing.

## Job

The Job `lab8-wikipedia-promotion-<target>` runs eight tasks in sequence:

```text
setup -> ingestion -> reference -> gold -> governance -> validation -> volume -> release_gate
```

| Task | What it does |
|---|---|
| `setup` | checks the migration ledger, applies new DDL, creates the metadata Volume, loads the reference snapshot and access mapping |
| `ingestion` | runs the triggered Wikipedia pipeline, without full refresh |
| `reference` | loads Wikimedia metadata and checks whether this is an initial build |
| `gold` | pins one Silver Delta version, deduplicates events, builds the fact, five dimensions and three aggregates |
| `governance` | creates policy functions, attaches row filters and the editor-name mask, grants analytical access |
| `validation` | checks unique keys, foreign keys, reconciliation, aggregate totals and attached policies |
| `volume` | records the current Gold volume against the last healthy baseline |
| `release_gate` | checks delivery completeness and repeatability, then records the successful release |

The Job has no schedule and allows one concurrent run. Empty Silver, missing policies or a failed mandatory check stop the release.
GitHub waits for the Job and fails if it fails. Gold table writes are separate operations; a failed release does not automatically roll them back.

## Release checks

The last tasks verify the data before the release is recorded:

```text
Bronze = Silver + Quarantine
Bronze count = distinct Kafka delivery count
Gold fact = distinct event IDs in the pinned Silver version
SUM(edit_count) of each aggregate = COUNT(fact)
```

Replayed events stay in Bronze and Silver as separate deliveries and become one fact row per `event_id` in Gold. Validation also checks keys, foreign keys, byte arithmetic and attached security policies, and the gate rejects a drop in Gold volume below the healthy baseline.

A successful run is recorded in `ops_releases` (run ID, deployed commit, Silver version, layer counts, fact checksum). For the same Silver version and Gold build, the gate compares the fact count and an `xxhash64` checksum with earlier releases. The checksum is a control, not a proof of equality. Migrations and volume history are kept in `ops_schema_migrations`, `ops_volume_log` and `ops_release_builds`.

## Repeatable deployments

- The bundle name, targets and workspace root are fixed, so every deployment reuses the same resources and state.
- SQL migrations are append-only and checksummed: an applied file cannot be edited or removed, changes go into a new migration. Repeated setup keeps the data and skips applied migrations.
- Bronze has `pipelines.reset.allowed=false` and the Job uses `full_refresh: false`. Lab 8 sets `failOnDataLoss=false`: if Event Hubs' retention expires, ingestion skips the lost offsets and continues. Events that expired before ingestion are not recovered, and the layer checks cannot detect them.
- DEV and PROD use separate checkpoints and Kafka `groupIdPrefix` values. Each reads Event Hub independently, so row counts differ between environments.
- Volume and release records are merged by run ID, so a retry does not duplicate them.

Deployment promotes code and configuration. Keeping PROD changes in Git also requires restricting manual write access to its resources.

## Results

Verified on 2026-10-11. All eight tasks succeeded in each run:

| Run | Bronze | Silver | Quarantine | Gold fact |
|---|---:|---:|---:|---:|
| [DEV: personal workspace](https://dbc-ab9a5afe-c151.cloud.databricks.com/jobs/639631853603919/runs/408896370548989) | 16,638 | 16,638 | 0 | 16,637 |
| [PROD: Azure trial 2](https://adb-7405618361895520.0.azuredatabricks.net/jobs/534890000465235/runs/475458704371265) | 5,572 | 5,572 | 0 | 5,572 |

The latest DEV run contains one replayed event, so Gold has one fewer row than Silver. The differing capture sizes come from the live source.

- Repeated DEV setup and refresh succeeded with existing tables, policies and checkpoints. The new migration created `ops_release_builds`, and the next setup skipped it.
- Strict PROD validation and deployment succeeded. A second deployment plan returned `skip` for every resource and permission.
- All four dashboard queries succeeded on both workspaces, and both dashboards were deployed.
- The 23-test local suite passed. Offline bundle checks and the workflow checks passed as well.

## Project layout

| Path | What it is |
|---|---|
| `databricks.yml` | Bundle variables and the `dev` and `prod` targets |
| `resources/` | Schemas, pipeline, promotion Job, dashboard and resource permissions |
| `pipeline/` | Wikipedia Bronze, Silver and quarantine datasets |
| `notebooks/` | Job task entry points and table operations |
| `src/lab8/` | Gold builder, migration parser and ledger, release contracts and fingerprints |
| `sql/migrations/` | Append-only operational and security DDL |
| `dashboards/`, `sql/dashboard/` | Dashboard JSON and its four dataset queries |
| `assets/` | Wikimedia reference snapshot and access policy |
| `tests/` | Local pytest suite |
| `tools/` | Offline checks, GitHub CI setup and personal-workspace helpers |

The bundle also uploads `lab5/src` and `lab6/src` from the repository. Their existing tables and deployed resources are kept unchanged.

## Targets

| Target | Workspace / compute | Catalog | Schema names |
|---|---|---|---|
| `dev` | Personal Free Edition, profile `personal-env`, serverless | `workspace` | `dev_ivanrazumovskyi_ivanrazumovskyi_lab8_silver`, `dev_ivanrazumovskyi_ivanrazumovskyi_lab8_gold` |
| `prod` | Azure trial 2, profile `azure_trial_2`, serverless | `dbr_dev` | `ivanrazumovskyi_lab8_silver`, `ivanrazumovskyi_lab8_gold` |

The CLI adds the `dev_<user>_` prefix to DEV schema resources. Job parameters, the Volume path and the dashboard use the resolved names.
Bronze, Silver and quarantine live in the Silver schema; analytical, reference, security and operational tables live in Gold. Wikipedia table names keep the `_lab5` suffix for compatibility with Lab 6.

The workspace, catalog, SQL warehouse, Event Hub and the secret `lab5-eventhub / eventhub-connection-string` must already exist. The deploying user needs access to the catalog, serverless compute and the warehouse, and permission to create Jobs and pipelines.

From `lab8/`, deploy and test on personal DEV:

```bash
python tools/personal_cli.py bundle validate --strict
python tools/personal_cli.py bundle deploy --select jobs.promotion_job
python tools/personal_cli.py bundle run promotion_job --params release_commit=local
python tools/validate_dashboard.py
python tools/personal_cli.py bundle deploy
```

The helper reads the ignored `.env.personal` and fixes the profile to `personal-env` and the target to `dev`. The first deploy selects the Job and its dependencies; the full deploy includes the dashboard after its queries have been checked against Gold data.

## GitHub Actions

`.github/workflows/lab8-ci.yml` uses PAT authentication and three secrets:

| Where | Secret | Value |
|---|---|---|
| Repository | `DATABRICKS_DEV_TOKEN` | personal workspace access token |
| Environment `lab8-prod` | `DATABRICKS_HOST` | `targets.prod.workspace.host` from `databricks.yml` |
| Environment `lab8-prod` | `DATABRICKS_TOKEN` | access token for the verified PROD owner |

The PROD job uses Environment `lab8-prod` for its host and token. The bundle defaults supply the deploying user, permissions, reader and PROD warehouse.

| Job | Runs on | What it does |
|---|---|---|
| `tests` | PR to `main`, relevant push to `main`, manual run | runs Ruff, checks the bundle offline and runs pytest on local Spark |
| `validate-dev` | same-repository PR, push to `main`, manual run | configures a host-only profile and runs strict DEV validation |
| `deploy-prod` | relevant push to `main`, after both checks pass | checks the host and PAT settings, validates and deploys PROD, runs the Job with `release_commit=github.sha` and waits |

Only the deployment job can read PROD secrets. Releases are queued, not cancelled, and `prod-summary.json` and `prod-run.log` are uploaded as artifacts, also on failure. The workflow runs only for changes in Lab 8, `lab5/src`, `lab6/src` and the workflow itself. To require review before PROD changes, protect `main` with PR review and the two checks.

## Local tests

Python 3.12, Java 17 and Databricks CLI 1.10.0, from the repository root:

```bash
pip install -r lab8/requirements-ci.txt
python -m ruff check lab8
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python -m pytest -c lab8/pytest.ini lab8/tests
python lab8/tools/check_bundle.py
```

Tests cover Wikipedia quality rules and replay deduplication, migration checksums and SQL splitting, build-scoped release checks, credential isolation and the DEV/PROD workflow boundaries. The offline checker validates the CLI resource schema, Python syntax, reference assets and consistency between dashboard SQL and JSON. Live data and governance checks run in the promotion Job.
