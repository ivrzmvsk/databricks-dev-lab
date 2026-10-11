# Lab 8 - CI/CD: DEV to PROD promotion

The Wikipedia work from Labs 5-7 as one Databricks Asset Bundle with two targets, `dev` (personal Free Edition) and `prod` (Azure workspace), and a GitHub Actions pipeline that tests every pull request and deploys and runs the bundle on merge to `main`.
Only Wikipedia is used. Netflix is not part of this lab. The Wikipedia producer runs outside the project (it sends events to the existing Event Hub) and is never started or configured here.

```text
PR to main    -> tests + offline bundle checks + bundle validate (dev)
merge to main -> bundle validate (prod) -> bundle deploy (prod) -> run promotion_job -> wait for success
```

## What was built

- **One bundle** `lab8_wikipedia_promotion` with the targets `dev` and `prod`. It reuses the Lab 5 parsing and expectations and the Lab 6 transformations (`lab5/src`, `lab6/src`) and writes only to its own two schemas.
- **Everything as code:** two Unity Catalog schemas, a Lakeflow pipeline, one eight-task Job, SQL migrations, a metadata Volume, row filters, a column mask and grants, an AI/BI dashboard with its four queries, and the Job and pipeline permissions.
- **A release gate** in the last task: Kafka delivery uniqueness, Bronze = Silver + quarantine, volume check, and a record of commit, run ID, Silver version, build fingerprint and fact checksum in `ops_releases`.
- **A CI/CD workflow** with a PROD deployment after checks pass on `main`: one release at a time and a GitHub Environment.

## Job

The Job `lab8-wikipedia-promotion-<target>` runs these tasks in sequence:

| Task | What it does |
|---|---|
| `setup` | applies new SQL migrations, creates the metadata Volume, loads the reference snapshot and the access mapping |
| `ingestion` | triggered Lakeflow pipeline: Event Hub -> Bronze -> Silver and quarantine (no full refresh) |
| `reference`, `gold` | five dimensions, the edit fact and three aggregates from one pinned Silver version |
| `governance` | default-deny row filters, editor-name mask, `SELECT` on the analytical tables only |
| `validation` | unique keys, foreign keys, reconciliation, aggregate totals, attached policies |
| `volume`, `release_gate` | volume baseline and delivery checks, then the release record |

There is no schedule. A failing check fails the task and the GitHub job. There is no automatic rollback, and the Gold writes are not one transaction.

## CI/CD

`.github/workflows/lab8-ci.yml`:

| Job | Runs on | What it does |
|---|---|---|
| `tests` | PR, push to `main`, manual | offline bundle checks (`tools/check_bundle.py`) and pytest on local Spark |
| `validate-dev` | PR (same repository), push | `bundle validate --strict -t dev` in the personal workspace |
| `deploy-prod` | push to `main` only, after `tests` and `validate-dev` | checks the PROD settings, `validate`, `deploy`, saves the inventory, runs `promotion_job` and waits for it |

- Only `deploy-prod` references PROD credentials (GitHub Environment `lab8-prod`); PR jobs do not consume them.
- Authentication uses PAT. No repository or environment variables are required.
- Releases are serialized (`concurrency`, no cancellation), and the Job allows one concurrent run.
- The PROD host is defined once, in `databricks.yml`; the workflow fails if the `DATABRICKS_PROD_HOST` secret differs from it.
- `prod-summary.json` and `prod-run.log` are uploaded as artifacts, also when the run fails.
- To make merges the only way to PROD, protect `main`: require PR review and the `tests` and `validate-dev` checks.

## Targets

| Target | Workspace / compute | Catalog | Schemas |
|---|---|---|---|
| `dev` | Free Edition (profile `personal-env`), serverless | `workspace` | `dev_ivanrazumovskyi_ivanrazumovskyi_lab8_silver`, `..._gold` |
| `prod` | Azure workspace (profile `azure_trial_2`), serverless, `mode: production` | `dbr_dev` | `ivanrazumovskyi_lab8_silver`, `ivanrazumovskyi_lab8_gold` |

The CLI adds the `dev_<user>_` prefix to DEV schema resources, so the DEV schema names look doubled. Notebook parameters, the Volume path and the dashboard use the resolved names. Lab 5 and Lab 6 schemas and deployed resources are only read. Bronze, Silver and quarantine live in the Silver schema; Gold, security, reference and operational tables live in the Gold schema. The `_lab5` suffix on the Wikipedia tables is kept to match the Lab 6 source contract.

## What is managed as code

| Asset | Source |
|---|---|
| Two schemas | `resources/lab8.schemas.yml` |
| Pipeline, Job and their permissions | `resources/`, target overrides in `databricks.yml` |
| Bronze, Silver, quarantine | `pipeline/` |
| Gold tables | `notebooks/gold.py`, `lab6/src` |
| Operational and security tables | `sql/migrations/*.sql` (append-only, checksummed) |
| Volume and reference metadata | `notebooks/setup.py`, `assets/wikimedia.json` |
| Row filters, column mask, grants | `notebooks/governance.py`, `assets/access_policy.json` |
| Dashboard and its four queries | `dashboards/`, `sql/dashboard/` |

The dashboard uses the viewer's own data permissions (`embed_credentials: false`). The Lab 6 alert and the destructive demo Jobs are not promoted. The workspace, catalog, warehouse, Event Hub and secret scope are prerequisites and are not created by the bundle. Terraform is not used.

## Repeatable deployments

- The bundle name, target and workspace root are stable, so resource IDs and deployment state are kept. PROD uses a fixed `prod_workspace_root`; a CI service principal needs `CAN_MANAGE` on that folder, and the root must stay the same when the deploying identity changes. The CLI version and the direct deployment engine are pinned.
- Migrations are `IF NOT EXISTS`, and an applied migration cannot be edited or removed (the checksum is verified). Schema changes go in a new migration. The runner splits statements outside quotes and (nested) comments, and an unclosed token fails before anything runs. `BEGIN ... END` blocks are not supported.
- Bronze has `pipelines.reset.allowed=false`, and the Job uses `full_refresh: false`: Event Hubs deletes old events, so Bronze cannot be rebuilt. Never destroy the bundle.
- DEV and PROD have separate checkpoints and different Kafka `groupIdPrefix` values.
- Gold deduplicates replayed events by `event_id`. Release and volume records are merged by run ID, so a retry does not duplicate them.
- For the same Silver version and the same build fingerprint (Gold transformations, shared Lab 6 and Lab 8 code, reference snapshot, access policy) the release gate compares the fact count and an order-independent `xxhash64` sum. A changed fingerprint starts a new baseline. The sum is a control, not a proof of equality, and different totals from a live source are not a failure.
- Event Hubs must keep the checkpointed offsets until the next run; an expired offset fails the run (`failOnDataLoss=true`) instead of losing data.
- Catalog administrators can override project ACLs. Keeping PROD free of manual changes also means limiting human write access there and deploying only through Git.

## Local checks

Python 3.12, Java 17 and Databricks CLI 1.10.0:

```bash
pip install -r lab8/requirements-ci.txt
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 python -m pytest -c lab8/pytest.ini lab8/tests
python lab8/tools/check_bundle.py
```

`check_bundle.py` validates the target definitions and resources against the CLI JSON schema, the Python syntax, the metadata assets and the dashboard SQL, without connecting to a workspace. It complements `bundle validate`; it does not prove permissions or runtime behavior.

The tests cover the environment boundaries (targets, schemas, compute, no schedules, `full_refresh: false`), the workflow guards (PROD only on push to `main`, PROD secrets only in `deploy-prod`), the migration parser and ledger, the release checks and the Wikipedia transformations.

## Deploy and run in DEV

```bash
cd lab8
databricks bundle validate --strict -t dev -p personal-env
databricks bundle deploy -t dev -p personal-env --select jobs.promotion_job
databricks bundle run promotion_job -t dev -p personal-env --params release_commit=local
python tools/validate_dashboard.py
databricks bundle deploy -t dev -p personal-env
```

The first deploy selects only the Job and the resources it depends on, so the dashboard queries can be checked on real Gold data before the dashboard is deployed. If the OAuth login has expired, run `databricks auth login --profile personal-env`, or put a token into the ignored `.env.personal` and call the CLI through `python tools/personal_cli.py ...`; the helper fixes the profile and the `dev` target.

## GitHub setup

Add these three repository secrets. PROD secrets may alternatively be stored in
the `lab8-prod` Environment used by the deployment job. If that Environment has
deployment restrictions, allow `main`; leave required reviewers unset for fully
automatic promotion after merge.

| Where | Name | Value |
|---|---|---|
| Repository secret | `DATABRICKS_DEV_TOKEN` | access token for the personal workspace |
| Repository secret | `DATABRICKS_PROD_HOST` | the PROD host from `databricks.yml` |
| Repository secret | `DATABRICKS_PROD_TOKEN` | access token for the already-tested PROD owner |

The workflow uses the Bundle defaults for run identity, management permissions,
reader (the authenticated PAT owner), and PROD warehouse (`d8b8ff9942968cb8`).
No OAuth client credentials or `LAB8_*` variables are needed. A qualifying push
to `main` automatically deploys and runs PROD after both prerequisite jobs pass.

The same identity deploys and runs the bundle. It needs `USE CATALOG` and `CREATE SCHEMA` on `dbr_dev`, access to the deployment folder, permission to create Jobs and pipelines, `CAN_USE` on the warehouse, and `READ` on the secret `lab5-eventhub / eventhub-connection-string` (Event Hub Listen access, broker reachable on TCP 9093). Tokens and secrets are never committed; `.env.*` files are ignored by Git.

## Results

Run on 2026-10-11 (run details and deployment plans are in the ignored `evidence/` folder):

- **DEV:** all eight tasks succeeded: Bronze, Silver and Gold each held 1,355 events and quarantine was empty. A second run against the existing resources also succeeded (5,813 unique Gold events as the live source grew), which shows the deployment is repeatable. After the review fixes (SQL splitting, build fingerprint, single PROD host) strict validation passed and a third run succeeded; the new migration created `ops_release_builds` and a repeated setup skipped it.
- **PROD (`azure_trial_2`):** strict validation passed, the bundle deployed and the Job ran with all eight tasks green: Bronze and Silver 5,572 events each, quarantine empty. A second deployment plan reported `skip` for every resource and permission, so redeploying changes nothing. PROD is still on this earlier deployment and does not yet contain the review fixes.
- All four dashboard queries ran on both workspaces, and the dashboards were deployed. Browser rendering has not been checked.
- 22 tests and the offline bundle checks pass.

Not done yet: a successful run of the merge-to-`main` workflow in GitHub Actions (the repository secrets and the Environment must be configured first), and the same-input checksum comparison, because the Silver version changed between the DEV runs. Direct CLI runs do not replace that acceptance test.

## Project layout

| Path | What it is |
|---|---|
| `databricks.yml` | Bundle: variables and the targets `dev` and `prod` |
| `resources/` | Schemas, pipeline, promotion Job, dashboard |
| `pipeline/` | Lakeflow sources: Bronze, Silver, quarantine |
| `notebooks/` | Task implementations: setup, reference, gold, governance, validation, volume, release gate |
| `src/lab8/` | Migration parser and ledger, release contracts |
| `sql/migrations/`, `sql/dashboard/` | Versioned DDL and the dashboard queries |
| `dashboards/`, `assets/` | Dashboard JSON, reference snapshot, access policy |
| `tests/` | pytest suite |
| `tools/` | Offline bundle and dashboard checks, personal-workspace helpers |
| `AGENTS.md` | Rules for AI assistants working in this folder |

References: [GitHub Actions CI/CD](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github), [bundle identities](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as), [Kafka group IDs and offsets](https://spark.apache.org/docs/4.1.2/streaming/structured-streaming-kafka-integration.html).
