"""Bounded Wikimedia -> local replay manifest -> Zerobus producer (DEV by default)."""

import argparse
import json
import logging
import time
from collections import Counter
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from requests_sse import EventSource
from zerobus.sdk.shared import StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

from lab11.common import ROOT, TARGETS, credentials, identifier, load_manifest

SOURCE = "https://stream.wikimedia.org/v2/stream/recentchange"
LOGGER = logging.getLogger(__name__)


def normalize(change):
    if not isinstance(change, dict) or not isinstance(change.get("meta", {}), dict):
        raise ValueError("Wikimedia event must be an object with object metadata")
    meta = change.get("meta", {})
    if meta.get("domain") == "canary" or change.get("type") != "edit":
        return None
    if not meta.get("id"):
        raise ValueError("Wikimedia event is missing meta.id")
    return {
        "event_id": str(meta["id"]),
        "wiki": str(change["wiki"]),
        "page_title": str(change["title"]),
        "user_name": str(change["user"]),
        "event_type": "edit",
        "bytes_changed": int(change["length"]["new"]) - int(change["length"]["old"]),
        "is_bot": bool(change["bot"]),
        "sequence_number": int(change["id"]),
        "event_timestamp": int(change["timestamp"]) * 1_000_000,
    }


def source_error():
    raise ConnectionError("Wikimedia stream interrupted; capture aborted")


def capture(count, timeout):
    records = {}
    counters = Counter()
    deadline = time.monotonic() + timeout
    # Disable implicit reconnect: a failed capture must be visible to the caller.
    with EventSource(
        SOURCE,
        timeout=10,
        max_connect_retry=0,
        on_error=source_error,
        headers={"User-Agent": "Lab11/1.0 (Databricks academic exercise)"},
    ) as source:
        for event in source:
            if time.monotonic() >= deadline:
                raise TimeoutError("Wikimedia capture exceeded deadline")
            if event.type != "message":
                continue
            try:
                record = normalize(json.loads(event.data))
            except (ValueError, KeyError, TypeError):
                counters["malformed"] += 1
                LOGGER.warning("Skipping malformed Wikimedia event (count=%d)", counters["malformed"])
                continue
            if not record:
                counters["filtered"] += 1
            if record:
                counters["duplicate"] += int(record["event_id"] in records)
                records[record["event_id"]] = record
            if len(records) == count:
                LOGGER.info("Capture complete: %s", dict(counters, accepted=len(records)))
                return list(records.values())
    raise RuntimeError("Wikimedia stream ended before requested event count")


def send(records, run_id, *, table_name=None, target="personal"):
    config = TARGETS[target]
    table_name = table_name or config["schema"] + ".bronze_wiki_events"
    identifier(table_name, 3)
    if not table_name.startswith(config["schema"] + "."):
        raise ValueError("Producer table must belong to the selected target schema")
    client_id, secret = credentials(target)
    ingest_host = config.get("ingest_host", config["host"])
    ingest_endpoint = config.get("ingest_endpoint", config["endpoint"])
    sdk = ZerobusSdk(ingest_endpoint, ingest_host)
    options = StreamConfigurationOptions(
        flush_timeout_ms=60_000,
        server_lack_of_ack_timeout_ms=30_000,
        recovery_retries=2,
        recovery_timeout_ms=15_000,
        max_inflight_records=1000,
    )
    stream = sdk.create_stream(client_id, secret, TableProperties(table_name), options)
    batch_id = str(uuid4())
    started = time.monotonic()
    try:
        for record in records:
            delivery = dict(
                record,
                producer_id="lab11-wikimedia",
                run_id=run_id,
                batch_id=batch_id,
                produced_at=time.time_ns() // 1000,
            )
            stream.ingest_record_offset(delivery)
        stream.flush()
    except Exception:
        # A failed graceful close must not mask the original ingestion failure.
        with suppress(Exception):
            stream.close()
        raise
    else:
        stream.close()
    return {
        "submitted": len(records),
        "ingest_host": ingest_host,
        "ingest_endpoint": ingest_endpoint,
        "table": table_name,
        "durability_confirmed": True,
        "batch_id": batch_id,
        "seconds": round(time.monotonic() - started, 3),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=tuple(TARGETS), default="personal")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--manifest", type=str)
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--duplicates", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.count < 1 or args.timeout < 1 or args.duplicates < 0:
        parser.error("count/timeout must be positive and duplicates nonnegative")
    if not args.replay and args.duplicates > args.count:
        parser.error("duplicates must not exceed count")
    logging.basicConfig(level=logging.INFO)
    if args.replay:
        if not args.manifest:
            parser.error("--replay requires --manifest")
        path = Path(args.manifest)
        manifest = load_manifest(path)
    else:
        run_id = "wiki-" + str(uuid4())
        path = Path(args.manifest) if args.manifest else ROOT / ".state" / f"{run_id}.json"
        if path.exists():
            parser.error("manifest already exists; choose another path or --replay")
        manifest = {"run_id": run_id, "source": SOURCE, "records": capture(args.count, args.timeout)}
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x") as output:
            json.dump(manifest, output, ensure_ascii=False, indent=2)
    records = manifest["records"]
    if not records or len({r["event_id"] for r in records}) != len(records):
        raise ValueError("Manifest must contain unique, nonempty event IDs")
    if args.duplicates > len(records):
        parser.error("duplicates must not exceed unique event count")
    report = {"manifest": str(path), "run_id": manifest["run_id"], "unique_events": len(records)}
    if not args.dry_run:
        report.update(send(records + records[: args.duplicates], manifest["run_id"], target=args.target))
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
