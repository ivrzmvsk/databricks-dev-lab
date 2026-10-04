"""Meaningful local tests: replay, wiki-scoped keys, UTC buckets, missing measures."""
import json
import os
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from pyspark.sql import SparkSession
from lab6.config import Config
from lab6.metadata import flatten_matrix
from lab6.transforms import prepare_edits, aggregate_activity


class GoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['TZ'] = 'UTC'
        time.tzset()
        cls.spark = SparkSession.builder.master('local[2]').appName('lab6-tests')\
            .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')\
            .config('spark.ui.enabled', 'false').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self):
        schema = ('event_id STRING, event_time STRING, wiki STRING, title STRING, user_name STRING, '
                  'namespace INT, bot BOOLEAN, old_length LONG, new_length LONG, old_revision LONG, '
                  'new_revision LONG, kafka_enqueued_at STRING, kafka_topic STRING, kafka_partition INT, kafka_offset LONG')
        rows = [
            ('a', '2026-10-04 15:04:59', 'enwiki', 'Same', 'Same', 0, False, 100, 150, 1, 2, '2026-10-04 15:05:00', 'events', 0, 1),
            ('a', '2026-10-04 15:04:59', 'enwiki', 'Same', 'Same', 0, False, 100, 150, 1, 2, '2026-10-04 15:05:01', 'events', 0, 2),
            ('b', '2026-10-04 15:05:00', 'plwiki', 'Same', 'Same', 0, True, 150, 100, 3, 4, '2026-10-04 15:05:01', 'events', 0, 3),
            ('c', '2026-10-04 15:05:10', 'plwiki', 'Other', 'Same', 1, False, None, 120, 4, 5, '2026-10-04 15:05:11', 'events', 0, 4),
        ]
        from pyspark.sql import functions as F
        return self.spark.createDataFrame(rows, schema).withColumn('event_time', F.to_timestamp('event_time'))

    def test_replay_keys_and_buckets(self):
        data = {r.event_id: r for r in prepare_edits(self.fixture()).collect()}
        self.assertEqual(len(data), 3)
        self.assertNotEqual(data['a'].page_key, data['b'].page_key)
        self.assertNotEqual(data['a'].editor_key, data['b'].editor_key)
        self.assertNotEqual(data['a'].namespace_key, data['b'].namespace_key)
        self.assertEqual(data['b'].editor_key, data['c'].editor_key)
        self.assertEqual(str(data['a'].bucket_start), '2026-10-04 15:00:00')
        self.assertEqual(str(data['b'].bucket_start), '2026-10-04 15:05:00')
        self.assertEqual(data['a'].date_key, 20261004)

    def test_measures_and_distinct_editors(self):
        edits = prepare_edits(self.fixture())
        rows = {r.event_id: r for r in edits.collect()}
        self.assertEqual((rows['a'].bytes_added, rows['a'].bytes_removed), (50, 0))
        self.assertEqual((rows['b'].bytes_added, rows['b'].bytes_removed), (0, 50))
        self.assertIsNone(rows['c'].bytes_delta)
        self.assertIsNone(rows['c'].bytes_added)
        agg = {r.wiki: r for r in aggregate_activity(edits, ['wiki']).collect()}
        self.assertEqual(agg['plwiki'].edit_count, 2)
        self.assertEqual(agg['plwiki'].editor_count, 1)
        self.assertEqual(agg['plwiki'].bot_share_pct, 50)
        self.assertEqual(agg['plwiki'].edits_with_lengths, 1)
        self.assertEqual(agg['plwiki'].net_bytes_delta, -50)

    def test_special_wikis_and_closed_flags(self):
        rows = flatten_matrix({'0': {'code':'en', 'name':'English', 'site':[
            {'dbname':'enwiki','code':'wiki','url':'https://en.wikipedia.org','closed':''}]},
            'specials':[{'dbname':'commonswiki','code':'commons','url':'https://commons.wikimedia.org'}], 'count':2})
        self.assertEqual(rows[0]['language_code'], 'en')
        self.assertTrue(rows[0]['is_closed'])
        self.assertIsNone(rows[1]['language_code'])
        self.assertFalse(rows[1]['is_closed'])

    def test_schema_isolation(self):
        with self.assertRaises(ValueError):
            Config(schema='ivanrazumovskyi_lab5')
        with self.assertRaises(ValueError):
            Config(schema='bad; DROP SCHEMA workspace')


if __name__ == '__main__':
    unittest.main()
