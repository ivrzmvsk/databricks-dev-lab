"""Transactional, additive seed and least-privilege Federation role."""

import argparse
import json
import secrets
from urllib.parse import unquote, urlsplit, urlunsplit

import psycopg2
from common import ROOT
from dotenv import dotenv_values
from psycopg2 import sql

OWNERS = [
    (1, "Lab Europe", "europe@example.invalid", "Europe"),
    (2, "Lab Americas", "americas@example.invalid", "Americas"),
    (3, "Lab Asia Pacific", "apac@example.invalid", "Asia Pacific"),
    (4, "Lab Global Services", "global@example.invalid", "Global"),
    (5, "Lab Community Support", "community@example.invalid", "Global"),
]
READERS = {
    "personal": ("lab10_federation_reader", ".env.reader"),
    "azure_trial_2": ("lab10_federation_reader_prod", ".env.reader.prod"),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=READERS, default="personal")
    args = parser.parse_args()
    role, reader_filename = READERS[args.target]
    env = dotenv_values(ROOT / ".env")
    supplied = env.get("DATABASE_URL_UNPOOLED") or env.get("DATABASE_URL")
    if not supplied:
        raise RuntimeError("Set DATABASE_URL in lab10/.env")
    parsed = urlsplit(supplied)
    if not parsed.hostname or not parsed.hostname.endswith(".neon.tech"):
        raise RuntimeError("Expected the selected Neon endpoint")
    # Neon direct and pooled endpoints differ only by the -pooler suffix.
    # Keep supplied env variables unchanged and use direct for schema work.
    host = parsed.hostname.replace("-pooler", "")
    database = unquote(parsed.path.lstrip("/"))
    netloc = f"{parsed.netloc.rsplit('@', 1)[0]}@{host}:{parsed.port or 5432}"
    direct = urlunsplit(parsed._replace(netloc=netloc))
    rows = json.loads((ROOT / "data/wiki_projects.json").read_text())
    if args.target == "azure_trial_2":
        rows += json.loads((ROOT / "data/wiki_projects_trial_additions.json").read_text())
    assert rows and len({r["wiki_id"] for r in rows}) == len(rows)
    reader_file = ROOT / reader_filename
    reader = dotenv_values(reader_file) if reader_file.exists() else {}
    if reader:
        expected = {
            "LAB10_READER_HOST": host,
            "LAB10_READER_PORT": str(parsed.port or 5432),
            "LAB10_READER_DATABASE": database,
            "LAB10_READER_USER": role,
        }
        if any(reader.get(key) != value for key, value in expected.items()):
            raise RuntimeError("Reader credentials belong to a different source; use separate env files")
    password = reader.get("LAB10_READER_PASSWORD") or secrets.token_urlsafe(32)
    conn = psycopg2.connect(direct, connect_timeout=15)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '30s'")
                cur.execute((ROOT / "sql/001_postgres.sql").read_text())
                cur.executemany(
                    """
                    INSERT INTO lab10.project_owners(owner_id,team_name,contact_email,region)
                    VALUES (%s,%s,%s,%s) ON CONFLICT (owner_id) DO NOTHING
                """,
                    OWNERS,
                )
                records = []
                for i, row in enumerate(rows):
                    records.append(
                        (
                            row["wiki_id"],
                            row.get("language_code") or "und",
                            row.get("language_name") or "Unknown",
                            row.get("project_code") or "unknown",
                            row.get("project_name") or row["wiki_id"],
                            (i % len(OWNERS)) + 1,
                            "active",
                            "priority" if row["wiki_id"] in ("enwiki", "ukwiki", "plwiki") else "standard",
                            "2026-10-12T00:00:00Z",
                        )
                    )
                cur.executemany(
                    """
                    INSERT INTO lab10.wiki_projects
                      (wiki_id,language_code,language_name,project_code,project_name,owner_id,status,support_tier,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (wiki_id) DO NOTHING
                """,
                    records,
                )
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
                if cur.fetchone():
                    if not reader:
                        raise RuntimeError(
                            "Reader role exists but local credential is missing; refusing to reset it"
                        )
                else:
                    # Store generated credential first so a process interruption does not lose it.
                    reader_file.touch(mode=0o600, exist_ok=True)
                    reader_file.chmod(0o600)
                    reader_file.write_text(
                        f'LAB10_READER_HOST="{host}"\nLAB10_READER_PORT="{parsed.port or 5432}"\n'
                        f'LAB10_READER_DATABASE="{database}"\n'
                        f'LAB10_READER_USER="{role}"\nLAB10_READER_PASSWORD="{password}"\n'
                    )
                    cur.execute(
                        sql.SQL(
                            "CREATE ROLE {} LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE"
                        ).format(sql.Identifier(role)),
                        (password,),
                    )
                cur.execute(sql.SQL("GRANT USAGE ON SCHEMA lab10 TO {}").format(sql.Identifier(role)))
                cur.execute(
                    sql.SQL("GRANT SELECT ON lab10.wiki_projects, lab10.project_owners TO {}").format(
                        sql.Identifier(role)
                    )
                )
                cur.execute("SELECT count(*) FROM lab10.wiki_projects")
                count = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM lab10.project_owners")
                owners = cur.fetchone()[0]
        print(json.dumps({"postgres_seed": "PASS", "projects": count, "owners": owners}))
    finally:
        conn.close()
    # Verify the reader can read and has no write privilege.
    check = psycopg2.connect(
        host=host,
        port=parsed.port or 5432,
        dbname=database,
        user=role,
        password=password,
        sslmode="verify-full",
        sslrootcert="/etc/ssl/certs/ca-certificates.crt",
        connect_timeout=15,
    )
    try:
        with check.cursor() as cur:
            cur.execute("SELECT count(*) FROM lab10.wiki_projects")
            assert cur.fetchone()[0] == count
            cur.execute(
                "SELECT has_table_privilege(current_user,'lab10.wiki_projects','INSERT,UPDATE,DELETE')"
            )
            assert not cur.fetchone()[0], "Federation reader must not write"
        print("Reader SELECT and lack of write privileges: PASS")
    finally:
        check.close()


if __name__ == "__main__":
    try:
        main()
    except psycopg2.Error as exc:
        # Driver errors can include connection material. Never print their text.
        raise SystemExit(f"Postgres failed: {type(exc).__name__}; SQLSTATE={exc.pgcode}") from None
