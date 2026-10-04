"""Source schemas and quality rules."""

TITLE_COLUMNS = [
    "show_id", "type", "title", "director", "cast", "country", "date_added",
    "release_year", "rating", "duration", "listed_in", "description",
]
TITLE_SCHEMA = ", ".join(f"`{name}` STRING" for name in TITLE_COLUMNS)
TITLE_SCHEMA += ", _corrupt_record STRING"

WIKI_SCHEMA = """
    meta STRUCT<id: STRING, domain: STRING, dt: STRING>,
    id BIGINT, type STRING, title STRING, user STRING, wiki STRING,
    timestamp BIGINT, namespace INT, bot BOOLEAN, server_name STRING,
    comment STRING, length STRUCT<old: BIGINT, new: BIGINT>,
    revision STRUCT<old: BIGINT, new: BIGINT>, _corrupt_record STRING
"""

TITLE_RULES = {
    "valid_csv": "_corrupt_record IS NULL",
    "valid_show_id": "show_id IS NOT NULL",
    "valid_title": "title IS NOT NULL",
    "valid_type": "content_type IN ('Movie', 'TV Show')",
    "valid_release_year": "release_year BETWEEN 1900 AND year(current_date()) + 1",
}

WIKI_RULES = {
    "valid_json": "_payload_valid",
    "valid_event_id": "event_id IS NOT NULL",
    "edit_event": "event_type = 'edit'",
    "valid_title": "title IS NOT NULL",
    "valid_user": "user_name IS NOT NULL",
    "valid_wiki": "wiki IS NOT NULL",
    "valid_domain": "domain IS NOT NULL AND domain <> 'canary'",
    "valid_timestamp": "event_time IS NOT NULL",
    "valid_lengths": "(old_length IS NULL OR old_length >= 0) AND (new_length IS NULL OR new_length >= 0)",
}

TITLE_BUSINESS_COLUMNS = [
    "show_id", "content_type", "title", "director", "cast", "country",
    "date_added", "release_year", "rating", "duration_raw", "genres", "description",
]
WIKI_BUSINESS_COLUMNS = [
    "event_id", "event_type", "title", "user_name", "wiki", "domain",
    "event_time", "namespace", "bot", "old_length", "new_length",
    "old_revision", "new_revision", "comment",
]
