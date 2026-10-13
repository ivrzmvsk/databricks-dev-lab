"""Linux-only live fault injection: shut down this process's Zerobus TCP socket."""

import argparse
import json
import os
import socket
import threading
import time
from urllib.parse import urlparse
from uuid import uuid4

from _acceptance import AcceptanceEnvironment
from zerobus.sdk.shared import AckCallback, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

from lab11.common import load_manifest


class Acks(AckCallback):
    def __init__(self):
        self.lock = threading.Lock()
        self.offsets = set()
        self.errors = 0

    def on_ack(self, offset):
        with self.lock:
            self.offsets.add(offset)

    def on_error(self, offset, error_message):
        with self.lock:
            self.errors += 1


def endpoint_sockets(endpoint):
    host = urlparse(endpoint).hostname
    addresses = {a[4][0] for a in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)}
    connected = []
    for name in os.listdir("/proc/self/fd"):
        duplicate = None
        duplicate_fd = None
        try:
            duplicate_fd = os.dup(int(name))
            duplicate = socket.socket(fileno=duplicate_fd)
            duplicate_fd = None
            peer = duplicate.getpeername()
            if (
                duplicate.family in (socket.AF_INET, socket.AF_INET6)
                and duplicate.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) == socket.SOCK_STREAM
                and peer[0] in addresses
                and peer[1] == 443
            ):
                connected.append(duplicate)
                duplicate = None
        except (OSError, ValueError):
            pass
        finally:
            if duplicate:
                duplicate.close()
            if duplicate_fd is not None:
                os.close(duplicate_fd)
    if not connected:
        raise RuntimeError("No matching Zerobus socket: fault was not injected")
    return connected


def main(argv=None, *, environment=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", help="Replay manifest; defaults to the committed Wikimedia sample")
    args = parser.parse_args(argv)
    env = environment or AcceptanceEnvironment()
    manifest = load_manifest(args.manifest)
    records = manifest["records"]
    client_id, secret = env.credentials()
    ack = Acks()
    sdk = ZerobusSdk(env.endpoint, env.host)
    stream = sdk.create_stream(
        client_id,
        secret,
        TableProperties(env.bronze),
        StreamConfigurationOptions(
            ack_callback=ack,
            recovery=True,
            recovery_retries=2,
            recovery_backoff_ms=500,
            recovery_timeout_ms=15_000,
            flush_timeout_ms=60_000,
            server_lack_of_ack_timeout_ms=10_000,
        ),
    )
    batch_id = "recovery-" + uuid4().hex
    sockets = endpoint_sockets(env.endpoint)
    offsets = []
    started = time.monotonic()
    try:
        # Single batch keeps the injected break ahead of its durability checkpoint.
        deliveries = [
            dict(
                r,
                producer_id="lab11-wikimedia",
                run_id=manifest["run_id"],
                batch_id=batch_id,
                produced_at=time.time_ns() // 1000,
            )
            for r in records
        ]
        offsets.append(stream.ingest_records_offset(deliveries))
        with ack.lock:
            unacked_at_break = len(set(offsets) - ack.offsets)
            if not unacked_at_break:
                raise AssertionError("Batch already acknowledged: recovery scenario not exercised")
            for connection in sockets:
                connection.shutdown(socket.SHUT_RDWR)
        print(
            json.dumps(
                {"socket_break_injected": True, "unacknowledged_submissions_at_break": unacked_at_break}
            ),
            flush=True,
        )
        stream.flush()
        stream.wait_for_offset(offsets[-1])
    finally:
        for connection in sockets:
            connection.close()
        stream.close()
    elapsed = round(time.monotonic() - started, 3)
    deadline = time.monotonic() + 120
    while True:
        row = env.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT event_id) FROM {env.bronze} WHERE batch_id = '{batch_id}'"
        )[0]
        count, unique = map(int, row)
        if count >= len(records) and unique == len(records):
            break
        if time.monotonic() > deadline:
            raise TimeoutError("Recovered batch is missing records")
        time.sleep(2)
    report = {
        "socket_break_injected": True,
        "unacknowledged_submissions_at_break": unacked_at_break,
        "durability_confirmed_after_break": True,
        "submitted_records": len(records),
        "bronze_deliveries": count,
        "bronze_unique_events": unique,
        "seconds_to_recover_and_flush": elapsed,
        "batch_id": batch_id,
        "terminal_error_callbacks": ack.errors,
    }
    assert ack.errors == 0
    env.save_report(f"{batch_id}.json", report)
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
