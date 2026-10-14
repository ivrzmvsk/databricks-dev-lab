"""Validate the frozen teaching corpus and create stable, token-bounded chunks."""

import hashlib
import html
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

import tiktoken


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def normalize(text: str) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", text)).split())


@dataclass(frozen=True)
class Document:
    doc_id: str
    name: str
    text: str
    genre: str
    coop: bool
    min_players: int
    max_players: int
    minutes: int
    min_age: int
    snapshot: str
    source_url: str

    @property
    def content_hash(self):
        return digest(json.dumps(asdict(self), sort_keys=True, ensure_ascii=False))


def load_documents(path: Path, expected_count: int = 12) -> list[Document]:
    manifest = json.loads(path.with_suffix(".manifest.json").read_text())
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != manifest["output_sha256"]:
        raise ValueError("Corpus checksum differs from selection manifest")
    docs = []
    for row in json.loads(path.read_text(encoding="utf-8"))["documents"]:
        doc = Document(**{**row, "text": normalize(row["text"])})
        if not re.fullmatch(r"BG\d{2}", doc.doc_id) or not doc.name.strip() or not doc.text.strip():
            raise ValueError("Missing or invalid game identity/text")
        if type(doc.coop) is not bool:
            raise ValueError("coop must be boolean")
        if any(
            type(v) is not int or v < 1
            for v in (doc.min_players, doc.max_players, doc.minutes, doc.min_age)
        ):
            raise ValueError("Invalid player, duration or age metadata")
        if doc.min_players > doc.max_players:
            raise ValueError("Invalid player range")
        if doc.source_url != f"dataset/board_games.json#{doc.doc_id}":
            raise ValueError("Source must identify the local fictional catalogue")
        docs.append(doc)
    if len(docs) != expected_count or len({d.doc_id for d in docs}) != len(docs):
        raise ValueError(f"Expected {expected_count} unique games")
    if sorted(manifest["document_ids"]) != sorted(d.doc_id for d in docs):
        raise ValueError("Selected doc_ids differ from manifest")
    return docs


def chunk_document(doc: Document, max_tokens: int = 400, overlap_tokens: int = 60):
    if not 0 <= overlap_tokens < max_tokens or max_tokens < 100:
        raise ValueError("Require max_tokens >= 100 and 0 <= overlap < max_tokens")
    enc = tiktoken.get_encoding("cl100k_base")
    prefix = (
        f"Fictional board game: {doc.name}\nDocument ID: {doc.doc_id}\nSnapshot: {doc.snapshot}\n"
        f"Genre: {doc.genre}\nCooperative: {doc.coop}\n"
        f"Players: {doc.min_players}-{doc.max_players}\n"
        f"Duration: {doc.minutes} minutes\nMinimum age: {doc.min_age}\nRules:\n"
    )
    if len(enc.encode(prefix)) > max_tokens - 32:
        raise ValueError("Metadata leaves insufficient room for description")
    spans = list(re.finditer(r"\S+", doc.text))
    start, ordinal, chunks = 0, 0, []
    while start < len(spans):
        low, high = start, len(spans)
        while low < high:
            mid = (low + high + 1) // 2
            candidate = doc.text[spans[start].start() : spans[mid - 1].end()]
            if len(enc.encode(prefix + candidate)) <= max_tokens:
                low = mid
            else:
                high = mid - 1
        end = low
        if end == start:
            raise ValueError("A single word exceeds the chunk token budget")
        body = doc.text[spans[start].start() : spans[end - 1].end()]
        text = prefix + body
        chunks.append(
            {
                "chunk_id": digest(
                    f"{doc.content_hash}:{max_tokens}:{overlap_tokens}:{ordinal}:{text}"
                ),
                "doc_id": doc.doc_id,
                "name": doc.name,
                "chunk_index": ordinal,
                "chunk_text": text,
                "source_url": doc.source_url,
                "snapshot": doc.snapshot,
                "document_hash": doc.content_hash,
                "token_count": len(enc.encode(text)),
                "start_char": spans[start].start(),
                "end_char": spans[end - 1].end(),
                "coop": doc.coop,
                "genre": doc.genre,
                "min_players": doc.min_players,
                "max_players": doc.max_players,
                "minutes": doc.minutes,
                "min_age": doc.min_age,
            }
        )
        ordinal += 1
        if end == len(spans):
            break
        next_start = end
        while next_start > start + 1:
            overlap = doc.text[spans[next_start - 1].start() : spans[end - 1].end()]
            if len(enc.encode(overlap)) > overlap_tokens:
                break
            next_start -= 1
        start = next_start
    return chunks


def prepare(path: Path, max_tokens=400, overlap_tokens=60):
    documents = load_documents(path)
    chunks = [
        chunk for doc in documents for chunk in chunk_document(doc, max_tokens, overlap_tokens)
    ]
    assert len({c["chunk_id"] for c in chunks}) == len(chunks)
    return documents, chunks
