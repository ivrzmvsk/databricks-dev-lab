"""Remove exactly one pair of isolated negative-test tables, never main tables."""

import argparse
import re

from _acceptance import AcceptanceEnvironment

from lab11.common import SCHEMA, identifier


def table_names(suffix, schema=SCHEMA):
    if not re.fullmatch(r"[0-9a-f]{12}", suffix):
        raise ValueError("Expected the 12-hex suffix printed by the negative test")
    identifier(schema, 2)
    return [f"{schema}.{prefix}_negative_{suffix}" for prefix in ("silver", "bronze")]


def main(argv=None, *, environment=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suffix", required=True)
    args = parser.parse_args(argv)
    env = environment or AcceptanceEnvironment()
    tables = table_names(args.suffix, env.schema)
    for table in tables:
        env.execute(f"DROP TABLE IF EXISTS {identifier(table, 3)}")
    print("Removed isolated test tables: " + ", ".join(tables))


if __name__ == "__main__":
    main()
