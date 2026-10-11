SELECT a.*, w.language_code, w.project_code FROM agg_wiki_daily a
      JOIN dim_wiki w ON a.wiki_key = w.wiki_key
