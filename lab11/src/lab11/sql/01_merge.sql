MERGE INTO ${silver} AS t
USING (
  SELECT event_id, wiki, page_title, user_name, event_type, bytes_changed, is_bot,
         sequence_number, event_timestamp, producer_id, run_id,
         batch_id AS first_batch_id, produced_at AS first_produced_at,
         COUNT(*) OVER (PARTITION BY event_id) AS delivery_count
  FROM ${bronze}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY produced_at NULLS LAST, batch_id NULLS LAST) = 1
) AS s
ON t.event_id = s.event_id
WHEN MATCHED AND (t.delivery_count IS DISTINCT FROM s.delivery_count
    OR t.first_produced_at IS DISTINCT FROM s.first_produced_at
    OR t.first_batch_id IS DISTINCT FROM s.first_batch_id)
THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
