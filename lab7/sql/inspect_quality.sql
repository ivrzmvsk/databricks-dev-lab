-- Personal workspace only. Replace the catalog/schema locally for other targets.
-- Named Lakeflow drop counters for the most recent update.
WITH latest AS (
  SELECT origin.update_id AS update_id
  FROM event_log(TABLE(workspace.ivanrazumovskyi_lab7.wiki_accepted))
  WHERE event_type = 'create_update'
  ORDER BY timestamp DESC LIMIT 1
), parsed AS (
  SELECT explode(from_json(details:flow_progress.data_quality.expectations,
    'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS rule
  FROM event_log(TABLE(workspace.ivanrazumovskyi_lab7.wiki_accepted)), latest
  WHERE event_type = 'flow_progress' AND origin.update_id = latest.update_id
)
SELECT rule.name, sum(rule.passed_records) AS passed_records,
       sum(rule.failed_records) AS failed_records
FROM parsed
WHERE rule.dataset LIKE '%wiki_accepted'
GROUP BY rule.name ORDER BY rule.name;

SELECT check_name, dimension, severity, observed, expected, passed, details, checked_at
FROM workspace.ivanrazumovskyi_lab7.dq_results
WHERE snapshot_id = (SELECT snapshot_id FROM workspace.ivanrazumovskyi_lab7.snapshot_manifest)
ORDER BY passed, severity, dimension, check_name;

SELECT count(*) AS quarantined_real_rows
FROM workspace.ivanrazumovskyi_lab7.wiki_quarantine;

-- Persisted synthetic rejection examples, separate from the real medallion.
SELECT event_id, event_json, kafka_offset, _quality_errors, demo_run_id
FROM workspace.ivanrazumovskyi_lab7.wiki_quarantine_demo
ORDER BY kafka_offset;

SELECT reason, count(*) AS affected_rows
FROM workspace.ivanrazumovskyi_lab7.wiki_quarantine
LATERAL VIEW explode(_quality_errors) failures AS reason
GROUP BY reason
ORDER BY affected_rows DESC;

SELECT event_id, kafka_topic, kafka_partition, kafka_offset, _duplicate_reason
FROM workspace.ivanrazumovskyi_lab7.wiki_duplicates;

SHOW TBLPROPERTIES workspace.ivanrazumovskyi_lab7.fact_edits;
SHOW CREATE TABLE workspace.ivanrazumovskyi_lab7.fact_edits;
-- DQX trusted fact violations (empty when the final gate passes).
SELECT event_id, _errors, _warnings, _snapshot_id
FROM workspace.ivanrazumovskyi_lab7.dqx_fact_quarantine;
