"""Build Wikimedia Gold from a pinned Silver snapshot using versioned Python files."""
from pyspark.sql import functions as F

from lab6.tables import replace_table
from lab6.transforms import aggregate_activity, prepare_edits
from lab8.releases import gold_build_fingerprint


def build_gold(spark, config, code_root):
    build_fingerprint = gold_build_fingerprint(code_root)
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
    spark.sql(f"ALTER TABLE {config.table('fact_edits')} SET TBLPROPERTIES "
              f"('lab6.source_version' = '{source_version}', 'lab8.build_fingerprint' = '{build_fingerprint}')")

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
    result = {'source_unique_events': count, 'first_event': str(coverage.first_event),
                'last_event': str(coverage.last_event)}
    return result
