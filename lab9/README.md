# Lab 9 — Wikipedia REST API automation

One Python command drives Wikipedia processing through the Databricks SDK:

```text
preflight -> setup Job -> pipeline update -> Gold Job -> JSON report
```

The runner waits for each exact run/update ID, starts Gold only after ingestion succeeds,
and exits `0` only after all stages succeed. Platform states, task states, notebook results,
run links and durations are saved even on failure. A single deadline bounds orchestration;
on timeout or interruption it requests cancellation of work it started. Cancellation is
reported as requested, not as verified completion. SDK HTTP requests have separate timeouts.
Bundle CLI subprocesses have a 20-minute timeout; failures return a one-line CLI error.

`automation.py` coordinates submission, a shared polling loop and cancellation.
`preflight.py` checks ownership and active runs; `reporting.py` owns report persistence
and SDK snapshots. CLI subcommands route to separate handlers through argparse.

## Data and resource ownership

Only Wikipedia is used. The bundle reuses Lab 5 parsing, Lab 6 Gold transformations and
the Wikipedia-only notebooks/pipeline from Lab 8. It creates its own Lab 9 schemas,
checkpoint state, pipeline and two Jobs, leaving Labs 5–8 deployed resources untouched.
The `lab8.*` configuration keys and `lab8_assets` Volume name are compatibility interfaces
of the reused code, not references to Lab 8's actual tables.

- `setup_job`: migrations, operational tables, reference assets and access mapping.
- `wiki_pipeline`: Event Hub -> Bronze -> Silver + quarantine, triggered with `full_refresh=false`.
- `gold_job`: reference -> Gold -> governance -> validation -> volume -> release gate.

Ingestion is called directly through `pipelines.start_update`, exactly once per successful
orchestration attempt, and is not repeated inside the Gold Job. Existing shared Event Hub
and workspace secrets must exist; its producer runs externally as in Lab 5. Retention gaps
follow Lab 8's `failOnDataLoss=false` behavior. An empty initial Silver dataset fails the
Gold build, so capture real Wikipedia events before the first end-to-end run.

## Personal environment

Python 3.12 and Databricks CLI 1.10.0 are required. From the repository root:

This checkout already has `lab9/.venv` configured. To recreate it, select Python 3.12
first (for this machine's pyenv installation, `pyenv shell 3.12.13`), then run:

```bash
python -m venv lab9/.venv
lab9/.venv/bin/python -m pip install -r lab9/requirements-dev.txt
lab9/.venv/bin/python -m pip install --no-deps -e lab9
lab9/.venv/bin/python -m lab9 validate
lab9/.venv/bin/python -m lab9 deploy
lab9/.venv/bin/python -m lab9 submit-notebook --report lab9/reports/compute.json
lab9/.venv/bin/python -m lab9 run --timeout 3600 --report lab9/reports/wikipedia.json
```

By default the CLI reads the existing ignored `lab8/.env.personal` without sourcing shell
code. In CI set `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. An explicit `--credentials-file`
overrides ambient credentials for personal. Personal is the default target; Azure trial 2
requires explicit `--target azure_trial_2` and either its pinned CLI profile or matching
`DATABRICKS_HOST` / `DATABRICKS_TOKEN` with `DATABRICKS_AUTH_TYPE=pat`. Each target has a
separate deployment inventory, and mismatched hosts or inventories are rejected.
Credentials never go into deployment inventory or reports.

`deploy` validates, deploys, and saves `.state/personal-inventory.json`. `inventory` refreshes
IDs from the existing deployment without deploying it. The runner verifies Job/pipeline
names and the notebook path before starting work and refuses to overlap existing runs.
Local operations have a file lock; CI operations share one concurrency group. These controls
do not constitute a distributed lock against independent UI/API users, so do not start
the same Lab 9 resources elsewhere while orchestration is running.

Additional commands:

```bash
lab9/.venv/bin/python -m lab9 pipeline --report lab9/reports/pipeline.json
lab9/.venv/bin/python -m lab9 status <run-id>
```

`pipeline` updates ingestion only; it does not refresh Gold. `status` reads one run without
starting anything. `submit-notebook` submits a one-time, bounded Spark probe via `jobs.submit`,
using the bundle-uploaded notebook. It creates no saved Job and deletes no shared resources.

## Compute requirement

The personal workspace is Free Edition. The bundle requests serverless pipeline compute
and submits notebook tasks without a classic-cluster specification. The runner does not
inspect the actual compute type, and reports contain no compute-type or cluster-creation
claims. This demonstrates programmatic workload submission; it does **not** demonstrate
literal classic-cluster creation. The assignment's “create
clusters” requirement remains subject to instructor acceptance of the serverless variant.
The Azure trial 2 target also requests serverless compute.

Databricks provisions and manages compute for the run; it is not a persistent user-created cluster.

## Week 8 CI/CD integration

`.github/workflows/lab9-ci.yml` follows Week 8's test/validation/deployment pattern
and consumes shared Lab 5/6/8 code directly. PRs and pushes with matching code or
configuration changes run lint, SDK-mocked tests and offline bundle checks. Changes
only to Lab 9 Markdown or saved evidence do not trigger this workflow.

After tests pass on a push to `main`, Actions uses **Azure trial 2 as PROD**:

```text
tests -> strict validate + deploy -> setup Job -> pipeline update -> Gold Job -> report
```

The live job uses the existing GitHub environment **`lab8-prod`** and its
`DATABRICKS_TOKEN` secret. Its host is pinned to the trial 2 workspace; the existing
`DATABRICKS_HOST` secret is not needed by Lab 9. Existing environment approval rules,
if configured, apply to this job. A missing PROD token fails before deployment; it
never falls back to the personal token. Temporary CLI profiles contain only host and
auth type; tokens remain in the environment and never enter artifacts.

Manual `workflow_dispatch` defaults to `deployment_target=azure_trial_2`, `operation=run`.
PROD execution is restricted to `main`. Choose `personal` to test another branch with
`DATABRICKS_DEV_TOKEN`; personal operations use environment `lab9-personal` and the
repository-level personal secret. `run` deploys the selected revision first, then executes
the complete sequence. `pipeline` and `submit-notebook` use the existing deployment:
dispatch `deploy` first when its code needs refreshing. `validate` and `deploy` do not
execute workloads. Operations for the same target share a concurrency group.
GitHub must have this workflow on the default branch before it offers manual dispatch.

Lab 8's original promotion behavior is restored: relevant pushes to `main` deploy its
PROD bundle and run `promotion_job` after checks pass. PRs and manual Lab 8 runs only check.
Lab 9 publishes the selected target's inventory and JSON reports as Actions artifacts.
The current workflow has been checked locally; it has not yet been pushed or run on GitHub.

Job parameters are defined once in `lab8/config/job-parameters.yml` and reused by the
Lab 8 promotion Job and both Lab 9 Jobs. Reports now use `format_version: 3`:
`stages[].outputs[task_key].result` contains the parsed notebook result, while `error`
and `trace` remain separate. Existing checked-in E2E evidence retains its original format.
The current contract is defined in [`schemas/report-v3.schema.json`](schemas/report-v3.schema.json)
and checked by tests. It rejects top-level `compute` and `classic_cluster_created` fields.
Historical v1/v2 evidence remains unchanged; its compute labels were configured assumptions,
not API measurements, and must not be used as proof of compute type. The v2 schema is kept
only for validating the historical v2 evidence.
Both Ctrl+C and SIGTERM request cancellation and save a `CANCELED` report; forced process
termination can prevent cleanup from completing.

## Azure trial 2: explicit deployment and testing

The `azure_trial_2` configuration uses the same workspace as Lab 8 PROD:
`https://adb-7405618361895520.0.azuredatabricks.net`, with the `azure_trial_2` profile
used in Lab 7. It requests serverless compute, catalog `dbr_dev` and the `lab5-eventhub`
secret scope, matching Lab 8. The owner should verify permissions, serverless availability
and the secret scope before deployment. Jobs have no schedules or
triggers; the pipeline is not continuous. Deploying configuration does not execute notebooks,
apply notebook SQL migrations or start ingestion.

The owner authorized deployment and runtime testing on this target. Authenticate the
`azure_trial_2` CLI profile to the pinned host first; the wrapper checks the profile host
before contacting Databricks and removes unrelated ambient Databricks credentials.
CI uses an explicit PAT environment instead, with the same host check and an isolated
temporary profile; it does not depend on the developer's OAuth cache.
From the repository root:

```bash
lab9/.venv/bin/python -m lab9 --target azure_trial_2 deploy
lab9/.venv/bin/python -m lab9 --target azure_trial_2 submit-notebook --report lab9/reports/azure-trial2-probe-v3.json
lab9/.venv/bin/python -m lab9 --target azure_trial_2 run --report lab9/reports/azure-trial2-e2e-v3.json
```

The production deployment path is pinned to the academy identity in `databricks.yml`.
Inventory is saved as `.state/azure_trial_2-inventory.json`; preflight checks resource
names and notebook ownership for the selected target. CI uses trial 2 on pushes to main;
personal remains available through manual dispatch.
The original academy DEV workspace is outside this lab's targets.

Live trial 2 verification passed: probe sum = 45; full orchestration completed in
347.836 seconds with all six Gold tasks successful. Bronze = Silver = Gold fact = 300,
quarantine = 0, release gate = SUCCEEDED. The first attempt correctly rejected empty
Silver; after refreshing Event Hub with the Lab 5 producer, the retry succeeded.
A subsequent CI-equivalent run using PAT authentication also succeeded in 347.012 seconds
without new producer events, preserving the same counts. See [validation details](VALIDATION.md)
and the [initial v3 report](evidence/azure-trial2-e2e.json).

## Local checks

```bash
lab9/.venv/bin/python -m ruff check lab9
lab9/.venv/bin/python -m pytest -c lab9/pyproject.toml lab9/tests
lab9/.venv/bin/python lab9/tools/check_bundle.py
```

The bundle checker uses the local CLI JSON schema without authenticating to either workspace.
It checks syntax and both targets' configuration shape; it cannot prove Azure permissions,
compute availability or runtime compatibility. Live validation results are recorded in `VALIDATION.md`.
