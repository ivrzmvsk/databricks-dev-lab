-- Read-only review queries for the personal DEV default schema.
-- Execute in a Databricks SQL warehouse after the LAB 10 DEV run.

-- Part A: live PostgreSQL rows through the foreign catalog.
SELECT wiki_id, language_name, project_code, owner_id, status
FROM ivanrazumovskyi_lab10_neon_dev.lab10.wiki_projects
ORDER BY wiki_id;

-- Hybrid join: existing Delta facts enriched with external operational data.
SELECT p.wiki_id, p.language_name, o.team_name, count(*) AS edits
FROM workspace.dev_ivanrazumovskyi_ivanrazumovskyi_lab9_gold.fact_edits f
JOIN ivanrazumovskyi_lab10_neon_dev.lab10.wiki_projects p ON f.wiki = p.wiki_id
JOIN ivanrazumovskyi_lab10_neon_dev.lab10.project_owners o ON p.owner_id = o.owner_id
GROUP BY p.wiki_id, p.language_name, o.team_name
ORDER BY edits DESC;

-- Part B: changes after the baseline (CDF retention applies).
SELECT wiki_id, owner_id, status, _change_type, _commit_version, _commit_timestamp
FROM table_changes('workspace.ivanrazumovskyi_lab10.wiki_projects_source', 1)
ORDER BY _commit_version, wiki_id, _change_type;

-- Permanent archive: preimages, postimages, inserts and deletes.
SELECT _change_type, count(*) AS events
FROM workspace.ivanrazumovskyi_lab10.wiki_change_events
GROUP BY _change_type
ORDER BY _change_type;

-- Every enwiki version, including the late business timestamp.
SELECT wiki_id, owner_id, status, updated_at, source_commit_version,
       valid_from, valid_to, valid_to_version, is_current
FROM workspace.ivanrazumovskyi_lab10.wiki_projects_history
WHERE wiki_id = 'enwiki'
ORDER BY source_commit_version;

SELECT * FROM workspace.ivanrazumovskyi_lab10.cdc_checkpoint;
