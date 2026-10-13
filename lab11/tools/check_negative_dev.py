"""Live schema/conflict checks in isolated tables; personal DEV by default."""

import argparse
import json
import time
from uuid import uuid4

from _acceptance import AcceptanceEnvironment
from zerobus.sdk.shared import ZerobusException

from lab11.common import load_manifest


def main(argv=None, *, environment=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", help="Replay manifest; defaults to the committed Wikimedia sample")
    args = parser.parse_args(argv)
    env = environment or AcceptanceEnvironment()
    manifest = load_manifest(args.manifest)
    suffix = uuid4().hex[:12]
    bronze, silver = f"{env.schema}.bronze_negative_{suffix}", f"{env.schema}.silver_negative_{suffix}"
    client_id, _ = env.credentials()
    for table, source in ((bronze, env.bronze), (silver, env.silver)):
        env.execute(f"CREATE TABLE {table} LIKE {source}")
    env.execute(f"GRANT SELECT, MODIFY ON TABLE {bronze} TO `{client_id}`")
    source = manifest["records"][0]
    print("Created isolated negative-test tables", flush=True)
    invalid = dict(source, bytes_changed="invalid-integer")
    started = time.monotonic()
    try:
        env.send([invalid], f"invalid-{suffix}", table_name=bronze)
    except ZerobusException as error:
        schema_error = type(error).__name__
    else:
        raise AssertionError("Invalid schema was unexpectedly acknowledged")
    elapsed = round(time.monotonic() - started, 3)
    assert int(env.execute(f"SELECT COUNT(*) FROM {bronze}")[0][0]) == 0
    print(json.dumps({"invalid_schema_rejected": schema_error, "seconds": elapsed}), flush=True)
    env.send([source], f"conflict-{suffix}", table_name=bronze)
    env.wait_rows(bronze, 1)
    env.process(bronze, silver)
    before = env.execute(f"SELECT * FROM {silver} ORDER BY event_id")
    conflicting = dict(source, page_title=source["page_title"] + " [intentional conflict test]")
    env.send([conflicting], f"conflict-{suffix}", table_name=bronze)
    env.wait_rows(bronze, 2)
    try:
        env.process(bronze, silver)
    except ValueError as error:
        assert str(error) == "Conflicting business payload for event_id"
    else:
        raise AssertionError("Conflicting ID was not rejected")
    assert env.execute(f"SELECT * FROM {silver} ORDER BY event_id") == before
    report = {
        "invalid_schema_rejected": True,
        "invalid_delivery_not_inserted": True,
        "invalid_schema_exception": schema_error,
        "invalid_schema_seconds": elapsed,
        "conflicting_id_rejected": True,
        "silver_unchanged_on_conflict": True,
        "bronze": bronze,
        "silver": silver,
    }
    env.save_report(f"negative-{suffix}.json", report)
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
