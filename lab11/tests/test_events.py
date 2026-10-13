import pytest

from lab11.producer import normalize


def event(**overrides):
    row = {
        "meta": {"id": "source-123", "domain": "en.wikipedia.org"},
        "type": "edit",
        "wiki": "enwiki",
        "title": "Example",
        "user": "Editor",
        "length": {"new": 150, "old": 100},
        "bot": False,
        "id": 42,
        "timestamp": 1700000000,
    }
    row.update(overrides)
    return row


def test_repeated_source_event_has_stable_identity_and_payload():
    first, second = normalize(event()), normalize(event())
    assert first == second
    assert first["event_id"] == "source-123"
    assert first["event_timestamp"] == 1700000000000000
    assert first["bytes_changed"] == 50


@pytest.mark.parametrize("row", [event(type="new"), event(meta={"domain": "canary"})])
def test_source_filters(row):
    assert normalize(row) is None


def test_missing_source_id_rejected():
    with pytest.raises(ValueError, match="missing meta.id"):
        normalize(event(meta={"domain": "en.wikipedia.org"}))
