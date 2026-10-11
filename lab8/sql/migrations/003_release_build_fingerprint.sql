CREATE TABLE IF NOT EXISTS {namespace}.ops_release_builds (
    run_id STRING NOT NULL, build_fingerprint STRING NOT NULL
) USING DELTA;
