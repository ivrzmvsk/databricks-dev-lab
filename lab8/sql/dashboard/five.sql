SELECT a.*, CAST(a.bucket_start AS DATE) AS event_date,
      w.language_code, w.project_code FROM agg_wiki_5min a
      JOIN dim_wiki w ON a.wiki_key = w.wiki_key
