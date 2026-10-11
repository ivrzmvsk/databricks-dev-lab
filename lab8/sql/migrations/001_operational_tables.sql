CREATE TABLE IF NOT EXISTS {namespace}.sec_user_access (
    username STRING NOT NULL, wiki STRING NOT NULL, can_view_editor BOOLEAN NOT NULL,
    access_reason STRING, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS {namespace}.ops_volume_log (
    run_id STRING, measured_at TIMESTAMP, scenario STRING, source_count BIGINT,
    observed_count BIGINT, expected_count BIGINT, drop_pct DOUBLE,
    threshold_pct DOUBLE, should_alert BOOLEAN, first_event TIMESTAMP,
    last_event TIMESTAMP, observed_5min_buckets BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS {namespace}.ops_releases (
    run_id STRING NOT NULL, release_commit STRING NOT NULL, checked_at TIMESTAMP,
    source_version BIGINT, bronze_count BIGINT, silver_count BIGINT,
    quarantine_count BIGINT, fact_count BIGINT, fact_checksum DECIMAL(38,0),
    status STRING
) USING DELTA;
