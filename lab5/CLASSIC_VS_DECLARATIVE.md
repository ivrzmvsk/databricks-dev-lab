# Classic Spark vs Lakeflow — Lab 5

## What is compared

The pipeline has two independent branches: the static Netflix CSV and the Wikipedia edits from Event Hub. They are not joined, there is no common key.

`lab5_05_classic_spark.ipynb` copies the pipeline inputs and results into Delta tables, repeats the same transformations in plain Spark with explicit writes and compares the business columns with `exceptAll` in both directions.
For Wikipedia it also compares Kafka offsets and quarantine reasons. Materialized views have no time travel, so the notebook copies their finished results.

Both sides use the same transformation code, so the fixture tests check the cleaning rules separately.

## Operational differences

| Aspect       | Lakeflow implementation                                   | Classic implementation                                            |
| ------------ | --------------------------------------------------------- | ----------------------------------------------------------------- |
| Definition   | Functions return DataFrames; decorators declare datasets  | Explicit reads, filtering and Delta writes                        |
| Dependencies | Graph inferred from reads between declared datasets       | Notebook execution order established manually                     |
| Kafka state  | Pipeline owns checkpoints                                 | Optional classic consumer owns a durable volume checkpoint        |
| Quality      | `expect_all_or_drop` metrics and a quarantine branch    | Explicit predicates, validation and quarantine writes             |
| Reload       | Selected downstream full refresh from retained raw Bronze | Recompute from frozen inputs and replace only classic outputs     |
| Lineage      | Pipeline graph plus Unity Catalog lineage                 | Unity Catalog can also capture supported classic Spark operations |
| Flexibility  | Dataset definitions must avoid actions and side effects   | General-purpose Spark code can coordinate custom actions          |
| Deployment   | Pipeline definition and source code in a bundle           | Classic job/notebook can also be packaged in a bundle             |

Unity Catalog lineage also works for classic Spark. What Lakeflow adds is dependency planning and pipeline monitoring. Both run the same Spark transformations.

## Safe rerun

A delivery is identified by `(kafka_topic, kafka_partition, kafka_offset)`. A rerun must not load the same delivery twice. The same Wikipedia `event_id` at different offsets is kept and reported, there is no business deduplication.

`wiki_bronze_lab5` cannot be reset, because Event Hub drops old messages. A downstream full refresh rebuilds from the saved Bronze. Resetting only a checkpoint can duplicate rows.

## Performance and cost

The lab uses triggered runs. Stop the producer before the no-new-input tests. `maxOffsetsPerTrigger` limits one microbatch, not the whole update.

Record the update duration and the classic transform + write duration, and note that they measure different things (the classic timer skips snapshots, comparison and the optional Kafka step). Startup time, compute type and data size affect both.

If you have access, check `system.billing.usage` for the pipeline DBUs. If billing is not visible or Free Edition limits apply, say so. Nothing is measured yet, so no claim about which approach is cheaper.

## Evidence

| Target                | Pipeline update   | Validation | Classic equality | Rerun/replay | Duration / DBUs                 |
| --------------------- | ----------------- | ---------- | ---------------- | ------------ | ------------------------------- |
| Personal Free Edition | Completed         | Passed     | Equal            | Passed       | Update 2m 9s, DBUs not measured |

## Official references

- [Python dataset definitions](https://docs.databricks.com/aws/en/ldp/developer/python-dev)
- [Expectations](https://docs.databricks.com/aws/en/ldp/expectations)
- [Refresh semantics](https://docs.databricks.com/aws/en/ldp/concepts/refresh)
- [Pipeline limitations, including time travel](https://docs.databricks.com/aws/en/ldp/limitations)
- [Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/pipelines-tutorial)
