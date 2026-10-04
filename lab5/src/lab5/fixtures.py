"""Synthetic Wikipedia events for tests and the quality demo."""
from copy import deepcopy
from datetime import datetime
import json


def wiki_fixture_rows():
    base = {
        "meta": {"id": "lab5-fixture-0", "domain": "en.wikipedia.org"},
        "id": 123, "type": "edit", "title": "Lakeflow", "user": "Lab5User",
        "wiki": "enwiki", "timestamp": 1700000000, "namespace": 0, "bot": False,
        "length": {"old": 100, "new": 110}, "revision": {"old": 1, "new": 2},
    }
    events = [deepcopy(base) for _ in range(6)]
    for index, event in enumerate(events):
        event["meta"]["id"] = f"lab5-fixture-{index}"
    events[1]["title"] = "   "
    events[2]["user"] = None
    events[3]["timestamp"] = -1
    events[4]["length"]["new"] = -5
    events[5]["meta"]["domain"] = "canary"
    payloads = [json.dumps(event) for event in events] + ["{invalid-json"]
    instant = datetime(2023, 11, 14, 22, 13, 20)
    return [(payload, "lab5-fixtures", 0, index, instant, instant) for index, payload in enumerate(payloads)]
