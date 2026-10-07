"""Policy audit must reject missing or incorrectly configured security."""
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from lab6.config import Config
from lab6.security import assert_policies
from lab6.tables import WIKI_TABLES


class PolicyChecks(unittest.TestCase):
    def setUp(self):
        self.config = Config()
        self.filters = [SimpleNamespace(table_name=t, catalog_name='workspace', schema_name=self.config.schema, filter_catalog='workspace',
            filter_schema=self.config.schema, filter_name='wiki_access',
            filter_col_usage='wiki') for t in WIKI_TABLES]
        self.masks = [SimpleNamespace(table_name='dim_editor', column_name='user_name', catalog_name='workspace', schema_name=self.config.schema, mask_catalog='workspace',
            mask_schema=self.config.schema, mask_name='editor_mask', mask_col_usage='wiki')]

    def audit(self):
        def sql(query):
            rows = self.filters if 'row_filters' in query else self.masks
            return SimpleNamespace(collect=lambda: [SimpleNamespace(asDict=lambda r=r: vars(r)) for r in rows])
        return assert_policies(SimpleNamespace(sql=sql), self.config)

    def test_expected_policies(self):
        self.assertEqual(self.audit()['status'], 'verified')

    def test_workspace_metadata_format(self):
        self.filters = [SimpleNamespace(table_name=t, table_catalog='workspace',
            table_schema=self.config.schema, filter_name=self.config.namespace + '.wiki_access',
            target_columns='wiki') for t in WIKI_TABLES]
        self.masks = [SimpleNamespace(table_name='dim_editor', column_name='user_name',
            table_catalog='workspace', table_schema=self.config.schema,
            mask_name=self.config.namespace + '.editor_mask', using_columns='wiki')]
        self.assertEqual(self.audit()['status'], 'verified')

    def test_missing_mask(self):
        self.masks = []
        with self.assertRaisesRegex(AssertionError, 'Missing editor mask'):
            self.audit()

    def test_wrong_row_filter_input(self):
        self.filters[0].filter_col_usage = 'title'
        with self.assertRaisesRegex(AssertionError, 'Unexpected row-filter inputs'):
            self.audit()


if __name__ == '__main__':
    unittest.main()
