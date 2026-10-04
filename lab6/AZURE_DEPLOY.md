# Azure Dev: deploy resources without running them

This target follows Lab 5: the same Azure workspace, catalog `dbr_dev`,
and classic compute with `Standard_D4ds_v5` and one worker. Lab 6 writes to
`dbr_dev.ivanrazumovskyi_lab6` and reads
`dbr_dev.ivanrazumovskyi_lab5.wiki_silver_lab5` when a Job is eventually run.
The Lab 5 pipeline is not redeployed or started by this bundle.

## Commands to run yourself

From the repository root, change to `lab6`. Replace the two placeholders with
the Azure profile you used for Lab 5 and an existing **Azure Dev** SQL warehouse ID.
The personal workspace warehouse ID is not used by this target.

```bash
cd lab6
LAB6_AZURE_PROFILE='replace-with-your-Lab5-Azure-profile'
LAB6_AZURE_WAREHOUSE_ID='replace-with-existing-Azure-warehouse-id'
databricks bundle validate --strict -t azure_dev --profile "$LAB6_AZURE_PROFILE" --var "warehouse_id=$LAB6_AZURE_WAREHOUSE_ID"
databricks bundle deploy -t azure_dev --profile "$LAB6_AZURE_PROFILE" --var "warehouse_id=$LAB6_AZURE_WAREHOUSE_ID"
```

Stop after `deploy`. These are the complete commands for the current request;
do not use `bundle run`, `jobs run-now`, notebook Run, or alert Evaluate.
Azure commands have not been executed as part of preparing this configuration.
Only offline YAML/schema and configuration checks were performed.

## What deployment creates

- Uploaded notebooks and shared Python code in the target's bundle files folder.
- The Gold Job, governance demo Job and volume-drop demo Job.
- The AI/BI dashboard, referencing Azure catalog/schema and the provided warehouse.
- The SQL volume-drop alert, subscribed to the deploying workspace user's email.

The Jobs have no schedules, continuous settings, or automatic triggers. The alert
schedule is explicitly `PAUSED`, and Azure target presets also pause triggers.
Job cluster definitions are created with the Jobs; deployment does not start
those clusters or run their notebook tasks. No dashboard refresh schedule is defined.

Gold tables, the metadata Volume, row filters, column masks, SQL functions and
table grants are created by notebook execution, **not by deployment**. Therefore
the dashboard will not have Gold data until the first separately authorized run,
and the alert is not evaluated now. Lab 6 does not create or start a SQL warehouse.

## Configuration for a future run

Azure uses a configurable `17.3.x-scala2.13` runtime and standard access mode
(`USER_ISOLATION`) for Unity Catalog governance. Runtime/node availability and
workspace cluster-policy requirements have not been checked against Azure.
If required, pass `azure_spark_version` or `azure_node_type` variable overrides.

The Azure Gold Job defaults to `refresh_reference=true`: during a future run it
will request SiteMatrix and SiteInfo from public Wikimedia APIs and save metadata
to its own Volume. No API request is made by deployment. Network access and
Wikimedia rate limits still need checking at execution time. If direct requests
are unavailable, provision the metadata snapshot separately and set the Job
parameter `refresh_reference=false` before that future run.

All three Jobs receive target-specific catalog/schema parameters. The default
Azure `reader_principal` is the deployer's workspace identity, avoiding the
personal test reader. Access grants themselves are applied only when Gold runs.

The personal target keeps its existing warehouse, `workspace` catalog, serverless
tasks, and snapshot-based reference loading. Always specify `-t azure_dev` for
this deployment; `personal` remains the default target.
