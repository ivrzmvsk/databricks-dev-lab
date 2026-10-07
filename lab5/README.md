# Lab 5 - Lakeflow Spark Declarative Pipelines

One pipeline with two independent sources, deployed with a Databricks Asset Bundle to Azure Dev, Azure trial and a personal Free Edition workspace:

- **Netflix CSV** (8,807 rows, batch) -> materialized views
- **Wikipedia edits** from the Azure Event Hub (streaming) -> streaming tables. The producer is the one from Lab 3.

```text
Netflix CSV -> titles_bronze_lab5 (MV) -> titles_silver_lab5 (MV + expectations)

Event Hub -> wiki_bronze_lab5 (raw JSON + Kafka offsets)
              -> wiki_prepared_lab5 (temporary view)
                  -> wiki_silver_lab5 (expectations)
                  -> wiki_quarantine_lab5 (rejected rows + reasons)
```

Rows that break a rule are dropped from Silver by `expect_all_or_drop` and kept in quarantine with the names of the broken rules.
`wiki_bronze_lab5` has `pipelines.reset.allowed=false`: Event Hub deletes old events, so Bronze cannot be rebuilt from the source.

## Project layout

| Path | What it is |
|---|---|
| `databricks.yml` | Bundle: variables and the targets `azure_dev`, `azure_trial`, `azure_trial_2` and `personal` |
| `resources/lab5.pipeline.yml` | Pipeline definition |
| `src/bronze.py`, `silver.py`, `quality_demo.py` | Pipeline datasets |
| `src/lab5/` | Shared code: schemas, quality rules, transformations |
| `notebooks/` | Setup, validation, quality demo, safe reload, classic Spark comparison |
| `producer/` | Wikimedia -> Event Hub producer (copy from Lab 3) |
| `tests/` | Local Spark tests for the shared transformations |
| [CLASSIC_VS_DECLARATIVE.md](CLASSIC_VS_DECLARATIVE.md) | Comparison with classic Spark |

## Targets

| Target | Workspace / compute | Catalog.schema |
|---|---|---|
| `azure_dev` | Azure Dev, classic compute | `dbr_dev.ivanrazumovskyi_lab5` |
| `personal` | Free Edition (profile `personal-env`), serverless | `workspace.ivanrazumovskyi_lab5` |
| `azure_trial` | Azure trial (profile `azure-trial`), serverless | `dbr_dev_trial.ivanrazumovskyi_lab5` |
| `azure_trial_2` | Second Azure trial (profile `azure_trial_2`), serverless | `dbr_dev.ivanrazumovskyi_lab5` |

The `azure_trial` and `azure_trial_2` targets follow their own CLI profiles and do not set `run_as`.

From `lab5/`, validate and deploy to trial:

```bash
databricks bundle validate --strict -t azure_trial -p azure-trial
databricks bundle deploy -t azure_trial -p azure-trial
```

Before running the pipeline, prepare its source CSV at
`/Volumes/dbr_dev_trial/ivanrazumovskyi_lab5/lab5_sources/netflix/netflix_titles.csv`
and its workspace secret `lab5-eventhub / eventhub-connection-string` with Listen
access to the existing Event Hub. Select `environment=azure_trial` in notebooks.

For the second trial workspace, use catalog `dbr_dev`:

```bash
databricks bundle validate --strict -t azure_trial_2 -p azure_trial_2
databricks bundle deploy -t azure_trial_2 -p azure_trial_2
databricks bundle run lab5_pipeline -t azure_trial_2 -p azure_trial_2
```

Before running, prepare the source CSV at
`/Volumes/dbr_dev/ivanrazumovskyi_lab5/lab5_sources/netflix/netflix_titles.csv`
and the secret `lab5-eventhub / eventhub-connection-string` in this workspace.
Select `environment=azure_trial_2` in notebooks.

## Producer

Run the unchanged producer locally from `lab5/`:

```bash
python3 -m venv .venv
.venv/bin/pip install -r producer/requirements.txt
.venv/bin/python producer/producer.py
```

If `producer/.env` is not configured yet, copy `producer/env.example` to it and
fill in `EVENTHUB_CONNECTION_STRING` with Send permission. Keep an existing
configured `.env`; no new connection string is needed just because a consumer
uses the trial workspace. The producer prints `Sent edit ...` and runs until
Ctrl+C. One producer supplies all workspace consumers through the same Event Hub.

## Results

Pipeline graph and lineage:

![lineage](screenshots/lineage.png)

Expectations on `titles_silver_lab5` (clean CSV, nothing dropped) and on the synthetic Wikipedia events from `quality_demo=true` (1 written, 6 dropped):

| Netflix Silver | Wikipedia fixture Silver |
|---|---|
| ![expectations](screenshots/expectations.png) | ![dropped](screenshots/expectations_dropped.png) |

## Local tests

```bash
pip install -r requirements-dev.txt
python tests/test_transforms.py
```
