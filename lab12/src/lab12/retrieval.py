"""Retrieval adapters with explicit filters and manifest-based result decoding."""

import json
import math
import re
import time
from dataclasses import dataclass

from databricks.sdk.errors import BadRequest

from lab12.config import Settings

BOOLEAN_FILTERS = {"coop"}
INTEGER_FILTERS = {"min_players", "max_players", "minutes", "min_age"}
FILTER_KEYS = BOOLEAN_FILTERS | INTEGER_FILTERS | {"doc_id", "name", "snapshot", "genre"}
COLUMNS = ["chunk_id", "doc_id", "name", "chunk_text", "source_url", "snapshot"]


@dataclass(frozen=True)
class Hit:
    chunk_id: str
    doc_id: str
    name: str
    chunk_text: str
    source_url: str
    snapshot: str
    score: float


def validate_filters(filters):
    filters = dict(filters or {})
    if set(filters) - FILTER_KEYS:
        raise ValueError("Unsupported metadata filter")
    for key, value in filters.items():
        if key in BOOLEAN_FILTERS:
            if type(value) is not bool:
                raise ValueError(f"{key} requires a boolean")
        elif key in INTEGER_FILTERS:
            if type(value) is not int or value < 1:
                raise ValueError(f"{key} requires a positive integer")
        elif not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} requires a nonempty string")
    return filters


def decode_results(response):
    names = [item["name"] for item in response["manifest"]["columns"]]
    score_key = "score" if "score" in names else "similarity_score"
    if not set(COLUMNS + [score_key]).issubset(names):
        raise ValueError("Index response omitted required columns")
    hits = []
    for values in response.get("result", {}).get("data_array", []) or []:
        if len(values) != len(names):
            raise ValueError("Index response width mismatch")
        row = dict(zip(names, values, strict=True))
        if any(row[k] is None for k in COLUMNS):
            raise ValueError("Index response has null document metadata")
        score = float(row[score_key])
        if not math.isfinite(score):
            raise ValueError("Invalid retrieval score")
        hits.append(Hit(**{key: str(row[key]) for key in COLUMNS}, score=score))
    return hits


class VectorRetriever:
    def __init__(self, workspace, settings: Settings):
        settings.assert_host(workspace.config.host)
        self.workspace, self.settings = workspace, settings

    def search(self, query, filters=None):
        filters = validate_filters(filters)
        # Management readiness can precede query readiness for a newly created index.
        deadline = time.monotonic() + 60
        while True:
            try:
                response = self.workspace.vector_search_indexes.query_index(
                    self.settings.table("chunks_index"),
                    columns=COLUMNS,
                    query_text=query,
                    query_type="HYBRID",
                    num_results=self.settings.top_k,
                    filters_json=json.dumps(filters) if filters else None,
                )
                break
            except BadRequest as exc:
                if "not ready" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                time.sleep(5)
        return decode_results(response.as_dict())


class LexicalPreview:
    """Offline inspection only: word overlap is not semantic Vector Search."""

    def __init__(self, chunks, top_k=5):
        self.chunks, self.top_k = chunks, top_k

    def search(self, query, filters=None):
        filters = validate_filters(filters)
        words = set(re.findall(r"\w+", query.casefold()))
        ranked = []
        for chunk in self.chunks:
            if any(chunk.get(key) != value for key, value in filters.items()):
                continue
            score = len(words & set(re.findall(r"\w+", chunk["chunk_text"].casefold())))
            if score:
                ranked.append(Hit(**{key: chunk[key] for key in COLUMNS}, score=float(score)))
        return sorted(ranked, key=lambda h: (-h.score, h.chunk_id))[: self.top_k]
