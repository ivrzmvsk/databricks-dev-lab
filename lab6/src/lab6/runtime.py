"""Gold builds and governance. All writes are scoped to the Lab 6 schema."""
import json
import uuid
from pathlib import Path
from pyspark.sql import functions as F
from lab6.metadata import fetch_snapshot
from lab6.transforms import aggregate_activity, prepare_edits, scoped_key

ANALYTICAL_TABLES = (
    "dim_wiki", "dim_date", "dim_namespace", "dim_page", "dim_editor", "fact_edits",
    "agg_wiki_5min", "agg_wiki_daily", "agg_page_activity",
)
WIKI_TABLES = tuple(t for t in ANALYTICAL_TABLES if t != "dim_date")
ALL_TABLES = ("ref_sitematrix_raw", "sec_user_access", "ops_volume_log") + ANALYTICAL_TABLES


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def replace_table(spark, config, name, frame):
    view = "_lab6_write_" + name
    frame.createOrReplaceTempView(view)
    # REPLACE preserves table policies and grants on Databricks.
    spark.sql(f"CREATE OR REPLACE TABLE {config.table(name)} USING DELTA AS SELECT * FROM {view}")
    spark.catalog.dropTempView(view)


def setup(spark, config):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.namespace}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.namespace}.lab6_assets")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.table('sec_user_access')} (
        username STRING NOT NULL, wiki STRING NOT NULL, can_view_editor BOOLEAN NOT NULL,
        access_reason STRING, updated_at TIMESTAMP
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {config.table('ops_volume_log')} (
        run_id STRING, measured_at TIMESTAMP, scenario STRING, source_count BIGINT,
        observed_count BIGINT, expected_count BIGINT, drop_pct DOUBLE,
        threshold_pct DOUBLE, should_alert BOOLEAN, first_event TIMESTAMP,
        last_event TIMESTAMP, observed_5min_buckets BIGINT
    ) USING DELTA""")
    owner = spark.sql("SELECT session_user() AS username").first().username
    # Initial provisioning only: reruns never replace an intentionally restricted role.
    spark.sql(f"""MERGE INTO {config.table('sec_user_access')} t USING
        (SELECT {sql_string(owner)} username, '*' wiki, true can_view_editor,
                'Lab 6 owner / job identity' access_reason, current_timestamp() updated_at) s
        ON t.username = s.username
        WHEN NOT MATCHED THEN INSERT *""")
    return owner


def load_reference(spark, config):
    if config.refresh_reference:
        observed = {r.wiki for r in spark.table(config.source).select('wiki').distinct().collect()}
        snapshot = fetch_snapshot(observed)
        try:
            path = Path(config.metadata_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(snapshot, ensure_ascii=False))
        except OSError:
            pass
    else:
        snapshot = json.loads(Path(config.metadata_path).read_text())
    rows = []
    for site in snapshot['sites']:
        rows.append(tuple(site.get(k) for k in [
            'wiki', 'language_code', 'language_name', 'project_code', 'site_name',
            'site_url', 'is_closed', 'is_private', 'raw_json',
        ]) + (json.dumps(snapshot.get('namespaces', {}).get(site['wiki'], {}), ensure_ascii=False),))
    schema = ('wiki STRING, language_code STRING, language_name STRING, project_code STRING, '
              'site_name STRING, site_url STRING, is_closed BOOLEAN, is_private BOOLEAN, '
              'raw_json STRING, namespaces_json STRING')
    frame = spark.createDataFrame(rows, schema).withColumn('fetched_at', F.current_timestamp())
    assert frame.select('wiki').distinct().count() == frame.count(), 'Duplicate SiteMatrix wiki IDs'
    replace_table(spark, config, 'ref_sitematrix_raw', frame)
    return {'sites': len(rows), 'namespace_errors': snapshot.get('namespace_errors', {})}


def build_gold(spark, config):
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    source_version = spark.sql(f'DESCRIBE HISTORY {config.source}').agg(F.max('version')).first()[0]
    edits = prepare_edits(spark.read.option('versionAsOf', source_version).table(config.source))
    count = edits.count()
    if count == 0:
        raise ValueError('Wikipedia Silver is empty; refusing to replace existing Gold')
    invalid = edits.filter('event_id IS NULL OR wiki IS NULL OR title IS NULL OR '
                           'user_name IS NULL OR namespace IS NULL OR event_time IS NULL').count()
    assert invalid == 0, 'Required Gold keys are missing'
    refs = spark.table(config.table('ref_sitematrix_raw'))
    wiki = edits.select('wiki', 'wiki_key').distinct().join(refs, 'wiki', 'left').select(
        'wiki_key', 'wiki', F.coalesce('language_code', F.lit('und')).alias('language_code'),
        F.coalesce('language_name', F.lit('Unknown / multilingual')).alias('language_name'),
        F.coalesce('project_code', F.lit('unknown')).alias('project_code'),
        F.coalesce('site_name', 'wiki').alias('site_name'), 'site_url',
        F.coalesce('is_closed', F.lit(False)).alias('is_closed'),
        F.col('raw_json').isNotNull().alias('metadata_matched'),
    )
    replace_table(spark, config, 'dim_wiki', wiki)
    dates = edits.agg(F.min('event_date').alias('lo'), F.max('event_date').alias('hi')).select(
        F.explode(F.sequence('lo', 'hi')).alias('date'))
    dates = dates.select(
        F.date_format('date', 'yyyyMMdd').cast('int').alias('date_key'), 'date',
        F.year('date').alias('year'), F.month('date').alias('month'),
        F.quarter('date').alias('quarter'), F.dayofweek('date').alias('day_of_week'),
        F.date_format('date', 'EEEE').alias('day_name'),
    )
    replace_table(spark, config, 'dim_date', dates)
    ns = edits.select('wiki', 'namespace', 'namespace_key').distinct().join(
        refs.select('wiki', 'namespaces_json'), 'wiki', 'left')
    # Namespace names come from each wiki's siteinfo, not from SiteMatrix.
    ns = ns.withColumn('_ns', F.from_json('namespaces_json',
                      'MAP<STRING,STRUCT<id:INT,canonical:STRING,`*`:STRING>>'))
    ns = ns.withColumn('_info', F.element_at('_ns', F.col('namespace').cast('string')))
    ns = ns.select('namespace_key', 'wiki', 'namespace',
        F.when(F.col('namespace') == 0, F.lit('Main content')).otherwise(
            F.coalesce(F.col('_info.canonical'), F.col('_info.`*`'),
                       F.concat(F.lit('Namespace '), F.col('namespace').cast('string')))
        ).alias('namespace_name'), F.col('_info').isNotNull().alias('metadata_matched'))
    replace_table(spark, config, 'dim_namespace', ns)
    pages = edits.groupBy('page_key', 'wiki', 'title').agg(
        F.min('event_time').alias('first_seen'), F.max('event_time').alias('last_seen'))
    replace_table(spark, config, 'dim_page', pages)
    editors = edits.groupBy('editor_key', 'wiki', 'user_name').agg(
        F.min('event_time').alias('first_seen'), F.max('event_time').alias('last_seen'))
    replace_table(spark, config, 'dim_editor', editors)
    fact = edits.select('event_id', 'event_time', 'event_date', 'bucket_start',
        'wiki', 'wiki_key', 'date_key', 'namespace_key', 'page_key', 'editor_key',
        'bot', 'edit_count', 'old_length', 'new_length', 'bytes_delta',
        'bytes_added', 'bytes_removed', 'old_revision', 'new_revision')
    replace_table(spark, config, 'fact_edits', fact)
    spark.sql(f"ALTER TABLE {config.table('fact_edits')} SET TBLPROPERTIES ('lab6.source_version' = '{source_version}')")
    coverage = edits.agg(F.min('event_time').alias('first_event'),
                         F.max('event_time').alias('last_event')).first()
    five = aggregate_activity(fact, ['wiki', 'wiki_key', 'date_key', 'bucket_start'])
    # Boundary buckets are partial. Internal buckets are only within the observed span;
    # that does not prove continuous ingestion, and missing buckets are not invented.
    five = five.withColumn('is_boundary_bucket',
        (F.col('bucket_start') < F.lit(coverage.first_event)) |
        (F.col('bucket_start') + F.expr('INTERVAL 5 MINUTES') > F.lit(coverage.last_event)))
    replace_table(spark, config, 'agg_wiki_5min', five)
    replace_table(spark, config, 'agg_wiki_daily',
                  aggregate_activity(fact, ['wiki', 'wiki_key', 'date_key', 'event_date']))
    replace_table(spark, config, 'agg_page_activity',
                  aggregate_activity(fact, ['wiki', 'wiki_key', 'date_key', 'event_date', 'page_key']))
    return {'source_unique_events': count, 'first_event': str(coverage.first_event),
            'last_event': str(coverage.last_event)}


def apply_governance(spark, config, reader=''):
    access = config.table('sec_user_access')
    spark.sql(f"""CREATE OR REPLACE FUNCTION {config.namespace}.wiki_access(row_wiki STRING)
        RETURNS BOOLEAN RETURN EXISTS (SELECT 1 FROM {access} a
        WHERE a.username = session_user() AND (a.wiki = row_wiki OR a.wiki = '*'))""")
    spark.sql(f"""CREATE OR REPLACE FUNCTION {config.namespace}.editor_mask(value STRING, row_wiki STRING)
        RETURNS STRING RETURN CASE WHEN EXISTS (SELECT 1 FROM {access} a
        WHERE a.username = session_user() AND a.can_view_editor
        AND (a.wiki = row_wiki OR a.wiki = '*')) THEN value ELSE '[MASKED]' END""")
    for table in WIKI_TABLES:
        spark.sql(f"ALTER TABLE {config.table(table)} SET ROW FILTER {config.namespace}.wiki_access ON (wiki)")
    spark.sql(f"ALTER TABLE {config.table('dim_editor')} ALTER COLUMN user_name "
              f"SET MASK {config.namespace}.editor_mask USING COLUMNS (wiki)")
    if reader:
        principal = '`' + reader.replace('`', '``') + '`'
        spark.sql(f'GRANT USE CATALOG ON CATALOG {config.catalog} TO {principal}')
        spark.sql(f'GRANT USE SCHEMA ON SCHEMA {config.namespace} TO {principal}')
        for table in ANALYTICAL_TABLES:
            spark.sql(f'GRANT SELECT ON TABLE {config.table(table)} TO {principal}')
        # No SELECT/MODIFY on raw references, access mappings, or operational logs.
    return {'row_filters': list(WIKI_TABLES), 'masked_column': 'dim_editor.user_name', 'reader': reader}


def validate_gold(spark, config):
    fact = spark.table(config.table('fact_edits'))
    n = fact.count()
    assert n > 0
    assert fact.select('event_id').distinct().count() == n, 'Duplicate events in fact'
    version = spark.sql(f"SHOW TBLPROPERTIES {config.table('fact_edits')} ('lab6.source_version')").first()[1]
    source_n = spark.read.option('versionAsOf', int(version)).table(config.source).select('event_id').distinct().count()
    assert source_n == n, 'Gold count differs from the pinned Silver snapshot'
    for table, key in [('dim_wiki','wiki_key'),('dim_date','date_key'),
                       ('dim_namespace','namespace_key'),('dim_page','page_key'),('dim_editor','editor_key')]:
        dim = spark.table(config.table(table))
        assert dim.count() == dim.select(key).distinct().count(), f'Duplicate {table} keys'
        assert dim.filter(F.col(key).isNull()).count() == 0
        assert fact.join(dim.select(key), key, 'left_anti').count() == 0, f'Orphan {key}'
    for table in ['agg_wiki_5min', 'agg_wiki_daily', 'agg_page_activity']:
        agg = spark.table(config.table(table))
        assert agg.agg(F.sum('edit_count')).first()[0] == n, f'{table} count mismatch'
        assert agg.agg(F.sum('net_bytes_delta')).first()[0] == fact.agg(F.sum('bytes_delta')).first()[0]
    assert fact.filter('bytes_delta IS NOT NULL AND bytes_delta <> bytes_added - bytes_removed').count() == 0
    counts = {t: spark.table(config.table(t)).count() for t in ALL_TABLES}
    return {'counts': counts, 'source_unique_events': source_n,
            'unmatched_wikis': spark.table(config.table('dim_wiki')).filter('NOT metadata_matched').count(),
            'unmatched_namespaces': spark.table(config.table('dim_namespace')).filter('NOT metadata_matched').count()}


def record_volume(spark, config, scenario='normal'):
    if scenario not in {'normal', 'simulated_drop', 'recovered'}:
        raise ValueError('Unknown volume scenario')
    fact = spark.table(config.table('fact_edits'))
    observed = fact.count()
    version = spark.sql(f"SHOW TBLPROPERTIES {config.table('fact_edits')} ('lab6.source_version')").first()[1]
    source_count = spark.read.option('versionAsOf', int(version)).table(config.source).select('event_id').distinct().count()
    prior = spark.table(config.table('ops_volume_log')).filter(
        "scenario IN ('normal','recovered') AND NOT should_alert")\
        .orderBy(F.col('measured_at').desc()).first()
    if scenario != 'normal' and prior is None:
        raise ValueError('Record a healthy baseline before running the volume demo')
    # A rebuild after source data loss must not silently lower the healthy baseline.
    expected = max(source_count, prior.expected_count if prior else 0)
    drop = max(0.0, 100.0 * (expected - observed) / expected) if expected else 0.0
    coverage = fact.agg(F.min('event_time'), F.max('event_time'), F.countDistinct('bucket_start')).first()
    row = (str(uuid.uuid4()), scenario, source_count, observed, expected, drop,
           config.drop_threshold_pct, bool(expected and drop >= config.drop_threshold_pct),
           coverage[0], coverage[1], coverage[2])
    schema = ('run_id STRING, scenario STRING, source_count LONG, observed_count LONG, '
              'expected_count LONG, drop_pct DOUBLE, threshold_pct DOUBLE, should_alert BOOLEAN, '
              'first_event TIMESTAMP, last_event TIMESTAMP, observed_5min_buckets LONG')
    spark.createDataFrame([row], schema).withColumn('measured_at', F.current_timestamp())\
        .select(*spark.table(config.table('ops_volume_log')).columns)\
        .write.format('delta').mode('append').saveAsTable(config.table('ops_volume_log'))
    return {'scenario': scenario, 'observed': observed, 'expected': expected, 'drop_pct': drop,
            'should_alert': row[7]}


def simulate_drop(spark, config):
    # Explicit demo action on Lab 6's fact only. Rebuild Gold immediately after evaluating the alert.
    assert spark.table(config.table('ops_volume_log')).filter("scenario = 'normal'").count() > 0
    spark.sql(f"DELETE FROM {config.table('fact_edits')} WHERE pmod(xxhash64(event_id), 10) <> 0")
    return record_volume(spark, config, 'simulated_drop')


def verify_governance(spark, config):
    """Exercise policies with the real session identity; restore access in finally."""
    username = spark.sql('SELECT session_user()').first()[0]
    access = config.table('sec_user_access')
    original = spark.table(access).filter(F.col('username') == username).collect()
    total = spark.table(config.table('fact_edits')).count()
    selected = spark.table(config.table('fact_edits')).groupBy('wiki').count().orderBy(F.col('count').desc()).first().wiki
    expected = spark.table(config.table('fact_edits')).filter(F.col('wiki') == selected).count()
    try:
        spark.sql(f'DELETE FROM {access} WHERE username = {sql_string(username)}')
        spark.sql(f"INSERT INTO {access} VALUES ({sql_string(username)}, {sql_string(selected)}, false, 'Temporary enforcement test', current_timestamp())")
        for table in WIKI_TABLES:
            rows = spark.table(config.table(table))
            assert rows.filter(F.col('wiki') != selected).count() == 0, f'RLS failed on {table}'
        restricted = spark.table(config.table('fact_edits')).count()
        assert restricted == expected
        assert spark.table(config.table('dim_editor')).filter("user_name <> '[MASKED]'").count() == 0
        masked = spark.table(config.table('dim_editor')).count()
        spark.sql(f'DELETE FROM {access} WHERE username = {sql_string(username)}')
        assert spark.table(config.table('fact_edits')).count() == 0, 'RLS must deny unmapped users'
        result = {'method': 'Real session identity, temporary access mapping; not a second-user test',
                  'allowed_wiki': selected, 'full_rows': total, 'restricted_rows': restricted,
                  'masked_editors': masked, 'unmapped_rows': 0, 'tables_checked': list(WIKI_TABLES)}
    finally:
        spark.sql(f'DELETE FROM {access} WHERE username = {sql_string(username)}')
        if original:
            spark.createDataFrame(original, spark.table(access).schema).write.mode('append').saveAsTable(access)
    assert spark.table(config.table('fact_edits')).count() == total, 'Owner access was not restored'
    return result
