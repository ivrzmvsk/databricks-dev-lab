"""Create only Lab 11 DEV tables and grant the supplied producer access."""

from uuid import UUID

from lab11.common import BRONZE, SCHEMA, credentials, dev_client, render_sql, sql, statements


def main():
    w = dev_client()
    client_id, _ = credentials()
    UUID(client_id)  # Validate before using the principal in SQL.
    for statement in statements(render_sql("00_create_tables.sql")):
        if statement.strip():
            sql(w, statement)
            print("DDL succeeded", flush=True)
    for statement in (
        f"GRANT USE CATALOG ON CATALOG workspace TO `{client_id}`",
        f"GRANT USE SCHEMA ON SCHEMA {SCHEMA} TO `{client_id}`",
        f"GRANT SELECT, MODIFY ON TABLE {BRONZE} TO `{client_id}`",
    ):
        sql(w, statement)
        print("Grant succeeded", flush=True)
    table = w.tables.get(BRONZE)
    assert table.table_type.value == "MANAGED" and table.data_source_format.value == "DELTA"
    grants = w.grants.get("table", BRONZE)
    privileges = {
        p.value
        for g in grants.privilege_assignments or []
        if g.principal == client_id
        for p in g.privileges or []
    }
    assert {"SELECT", "MODIFY"} <= privileges
    print("Verified: managed Delta and explicit SELECT/MODIFY grants", flush=True)


if __name__ == "__main__":
    main()
