SELECT f.event_id, f.event_time, f.event_date, f.bucket_start, f.wiki,
      w.language_code, w.project_code, w.site_name, p.title, e.user_name,
      n.namespace_name, f.editor_key, f.page_key, f.bytes_delta,
      CASE WHEN f.bot THEN 'Bot' ELSE 'Human / unspecified' END AS editor_type
    FROM fact_edits f
    JOIN dim_wiki w ON f.wiki_key = w.wiki_key
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_page p ON f.page_key = p.page_key
    JOIN dim_editor e ON f.editor_key = e.editor_key
    JOIN dim_namespace n ON f.namespace_key = n.namespace_key
