# Lab 8 — Wikipedia promotion

Use databricks-core, databricks-dabs and the matching product skills.
Only `dev` / `personal-env` may be contacted by an agent. Always specify both
target and profile. Never execute Azure commands, even read-only validation.
The owner configures GitHub Actions to deploy and run `prod` after merge.
PROD is the Azure workspace from profile `azure_trial_2`, using serverless compute.
Its canonical host is `targets.prod.workspace.host` in `databricks.yml`.
The former academy `azure-dev` workspace is no longer a Lab 8 deployment target.

Write only to the Lab 8 schemas. Lab 5/6 data and deployed resources are read-only.
Reuse their transformation libraries; do not modify their bundles or notebooks.
Ignore lab88888. Never start or configure the external Wikipedia producer.
Keep secrets out of tracked files and logs. The owner's ignored `.env.personal`
may be loaded by `tools/personal_cli.py`; never load `.env.prod` as an agent.
No full refresh of Event Hubs Bronze.
Validate dashboard queries on personal data before deploying its resource.
