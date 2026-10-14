from dataclasses import replace
from pathlib import Path

import pytest
import tiktoken

from lab12.corpus import Document, chunk_document, load_documents, prepare

CORPUS = Path(__file__).resolve().parents[1] / "dataset/board_games.json"


@pytest.fixture(scope="module")
def documents():
    return load_documents(CORPUS)


def test_frozen_corpus_and_metadata(documents):
    assert len(documents) == 12
    assert len({d.doc_id for d in documents}) == 12
    assert all(d.genre and d.min_players <= d.max_players for d in documents)


def test_corpus_checksum_rejects_edits(tmp_path):
    copy = tmp_path / CORPUS.name
    copy.write_bytes(CORPUS.read_bytes() + b"\n")
    copy.with_suffix(".manifest.json").write_bytes(
        CORPUS.with_suffix(".manifest.json").read_bytes()
    )
    with pytest.raises(ValueError, match="checksum"):
        load_documents(copy)


@pytest.mark.parametrize("budget,overlap", [(250, 40), (400, 60), (700, 80)])
def test_chunk_coverage_bounds_and_repeatability(documents, budget, overlap):
    enc = tiktoken.get_encoding("cl100k_base")
    for doc in documents:
        chunks = chunk_document(doc, budget, overlap)
        assert chunks == chunk_document(doc, budget, overlap)
        assert chunks[0]["start_char"] == 0
        assert chunks[-1]["end_char"] == len(doc.text)
        assert all(c["token_count"] == len(enc.encode(c["chunk_text"])) <= budget for c in chunks)
        assert all(c["doc_id"] == doc.doc_id and c["name"] == doc.name for c in chunks)
        for left, right in zip(chunks, chunks[1:], strict=False):
            assert right["start_char"] <= left["end_char"] + 1
            assert right["start_char"] > left["start_char"]


def test_metadata_change_invalidates_chunk_ids(documents):
    doc = documents[0]
    changed = replace(doc, coop=not doc.coop)
    assert chunk_document(doc)[0]["chunk_id"] != chunk_document(changed)[0]["chunk_id"]


def test_unicode_preserved():
    doc = Document(
        "BG01",
        "Игра™",
        ("Мир океана 🐟 " * 200).strip(),
        "Strategy",
        True,
        2,
        4,
        25,
        8,
        "fictional-v1",
        "dataset/board_games.json#BG01",
    )
    chunks = chunk_document(doc)
    assert all("\ufffd" not in c["chunk_text"] for c in chunks)
    assert chunks[-1]["end_char"] == len(doc.text)


@pytest.mark.parametrize("budget,overlap", [(50, 0), (400, 400), (400, -1)])
def test_invalid_chunk_parameters(budget, overlap, documents):
    with pytest.raises(ValueError):
        chunk_document(documents[0], budget, overlap)


def test_prepared_chunk_keys_are_unique():
    docs, chunks = prepare(CORPUS)
    assert len({c["chunk_id"] for c in chunks}) == len(chunks)
    assert {c["doc_id"] for c in chunks} == {d.doc_id for d in docs}
    assert all(c["genre"] for c in chunks)
