-- Own lab schema only. Never drops, truncates, or replaces existing objects.
CREATE SCHEMA IF NOT EXISTS lab10;

CREATE TABLE IF NOT EXISTS lab10.project_owners (
    owner_id INTEGER PRIMARY KEY,
    team_name TEXT NOT NULL UNIQUE,
    contact_email TEXT NOT NULL,
    region TEXT NOT NULL,
    is_synthetic BOOLEAN NOT NULL DEFAULT TRUE CHECK (is_synthetic)
);

CREATE TABLE IF NOT EXISTS lab10.wiki_projects (
    wiki_id TEXT PRIMARY KEY,
    language_code TEXT NOT NULL,
    language_name TEXT NOT NULL,
    project_code TEXT NOT NULL,
    project_name TEXT NOT NULL,
    owner_id INTEGER NOT NULL REFERENCES lab10.project_owners(owner_id),
    status TEXT NOT NULL CHECK (status IN ('active', 'maintenance', 'retired')),
    support_tier TEXT NOT NULL CHECK (support_tier IN ('standard', 'priority')),
    updated_at TIMESTAMPTZ NOT NULL,
    ownership_is_synthetic BOOLEAN NOT NULL DEFAULT TRUE CHECK (ownership_is_synthetic)
);
COMMENT ON TABLE lab10.wiki_projects IS
    'LAB 10: observed Wikimedia metadata with fictional team ownership, status and support tier';
COMMENT ON TABLE lab10.project_owners IS
    'LAB 10: fictional teams and example.invalid contacts; not actual Wikimedia ownership';
