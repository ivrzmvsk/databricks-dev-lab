"""Read-only policy checks shared by refresh, validation and security demo."""
from lab6.tables import WIKI_TABLES


def _scope(row, config):
    # Runtime versions expose either table_catalog/table_schema or
    # catalog_name/schema_name in these information-schema relations.
    return (row.get('table_catalog', row.get('catalog_name')) == config.catalog
            and row.get('table_schema', row.get('schema_name')) == config.schema)


def _function(row, kind):
    name = row[kind + '_name']
    if kind + '_catalog' in row:
        return '.'.join((row[kind + '_catalog'], row[kind + '_schema'], name))
    return name


def assert_policies(spark, config):
    filters = [r.asDict() for r in spark.sql(
        f'SELECT * FROM {config.catalog}.information_schema.row_filters').collect()]
    by_table = {r['table_name']: r for r in filters if _scope(r, config)}
    for table in WIKI_TABLES:
        row = by_table.get(table)
        assert row is not None, f'Missing row filter on {table}; run the governance Job'
        assert _function(row, 'filter') == config.namespace + '.wiki_access', f'Unexpected row filter on {table}'
        inputs = row.get('filter_col_usage', row.get('target_columns'))
        assert inputs == 'wiki', f'Unexpected row-filter inputs on {table}'

    masks = [r.asDict() for r in spark.sql(
        f'SELECT * FROM {config.catalog}.information_schema.column_masks').collect()]
    masks = [r for r in masks if _scope(r, config) and r['table_name'] == 'dim_editor'
             and r['column_name'] == 'user_name']
    assert len(masks) == 1, 'Missing editor mask; run the governance Job'
    mask = masks[0]
    assert _function(mask, 'mask') == config.namespace + '.editor_mask', 'Unexpected editor mask'
    inputs = mask.get('mask_col_usage', mask.get('using_columns'))
    assert inputs == 'wiki', 'Unexpected editor-mask inputs'
    return {'status': 'verified', 'row_filters': list(WIKI_TABLES),
            'masked_column': 'dim_editor.user_name'}
