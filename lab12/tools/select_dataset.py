"""Select ten fixed games from the original Kaggle CSV for a small RAG demo."""

import csv
import hashlib
import html
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "dataset/games_march2025_cleaned.csv"
OUTPUT = ROOT / "dataset/steam_rag_10.csv"
APPIDS = (
    "264710",
    "413150",
    "427520",
    "294100",
    "526870",
    "2379780",
    "1868140",
    "945360",
    "367520",
    "1145360",
)
FIELDS = [
    "appid",
    "name",
    "detailed_description",
    "about_the_game",
    "short_description",
    "genres",
    "categories",
    "supported_languages",
    "windows",
    "mac",
    "linux",
]


def main():
    csv.field_size_limit(20_000_000)
    selected = {}
    with SOURCE.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle, strict=True):
            if row["appid"] not in APPIDS:
                continue
            text = " ".join(
                html.unescape(re.sub(r"<[^>]+>", " ", row["detailed_description"])).split()
            )
            if not text:
                raise ValueError(f"Empty description: {row['appid']}")
            selected[row["appid"]] = {
                **{k: row[k] for k in FIELDS},
                "dataset_snapshot": "2025-03",
                "document_sha256": hashlib.sha256(text.encode()).hexdigest(),
            }
    if set(selected) != set(APPIDS):
        raise ValueError("Source is missing a selected game")
    with OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS + ["dataset_snapshot", "document_sha256"])
        writer.writeheader()
        writer.writerows(selected[appid] for appid in APPIDS)
    with SOURCE.open("rb") as handle:
        source_hash = hashlib.file_digest(handle, "sha256").hexdigest()
    manifest = {
        "source": SOURCE.name,
        "source_sha256": source_hash,
        "output": OUTPUT.name,
        "output_sha256": hashlib.sha256(OUTPUT.read_bytes()).hexdigest(),
        "selected_rows": len(APPIDS),
        "selected_appids": list(APPIDS),
        "selection": "Fixed ten-game teaching sample; full descriptions, no synthetic text",
        "limitations": [
            "Not representative of Steam",
            "March 2025 snapshot, not current information",
        ],
    }
    OUTPUT.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(
        json.dumps(
            {
                "games": len(selected),
                "bytes": OUTPUT.stat().st_size,
                "names": [selected[a]["name"] for a in APPIDS],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
