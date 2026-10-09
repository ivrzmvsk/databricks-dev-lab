"""Fixed Wikipedia fault injection; written only to isolated Lab 7 tables."""

import json
from datetime import datetime

from lab5.fixtures import wiki_fixture_rows


def demo_rows():
    rows = wiki_fixture_rows()
    event = json.loads(rows[0][0])
    event["meta"]["id"] = "lab7-fixture-valid"
    event["meta"]["domain"] = "en.wikipedia.org"
    event["title"] = "Lab 7 fixture"
    event["user"] = "Lab7User"
    instant = datetime(2023, 11, 14, 22, 13, 20)
    # Two accepted deliveries with the same ID: one becomes a duplicate.
    rows += [
        (json.dumps(event), "lab7-fixtures", 0, 100, instant, instant),
        (json.dumps(event), "lab7-fixtures", 0, 101, instant, instant),
    ]
    return rows
