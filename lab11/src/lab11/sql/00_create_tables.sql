CREATE SCHEMA IF NOT EXISTS ${schema};

-- Bronze: append-only, written only by Zerobus, one row per delivery (duplicates included).
CREATE TABLE IF NOT EXISTS ${bronze} (
  event_id        STRING    COMMENT 'Wikimedia meta.id or deterministic fixture ID, stable across retries and runs',
  wiki            STRING,
  page_title      STRING,
  user_name       STRING,
  event_type      STRING,
  bytes_changed   BIGINT,
  is_bot          BOOLEAN,
  sequence_number BIGINT,
  event_timestamp TIMESTAMP,
  producer_id     STRING,
  run_id          STRING,
  batch_id        STRING    COMMENT 'delivery metadata that differs between duplicate deliveries',
  produced_at     TIMESTAMP COMMENT 'delivery metadata that differs between duplicate deliveries'
);

-- Silver: one row per event_id, maintained by the idempotent MERGE.
CREATE TABLE IF NOT EXISTS ${silver} (
  event_id        STRING,
  wiki            STRING,
  page_title      STRING,
  user_name       STRING,
  event_type      STRING,
  bytes_changed   BIGINT,
  is_bot          BOOLEAN,
  sequence_number BIGINT,
  event_timestamp TIMESTAMP,
  producer_id     STRING,
  run_id          STRING,
  first_batch_id  STRING,
  first_produced_at TIMESTAMP,
  delivery_count  BIGINT
);
