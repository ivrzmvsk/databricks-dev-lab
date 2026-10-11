WITH top_pages AS (
      SELECT page_key FROM agg_page_activity
      GROUP BY page_key ORDER BY SUM(edit_count) DESC, page_key LIMIT 20
    )
    SELECT a.*, p.title, CONCAT(a.wiki, ': ', p.title) AS page_label,
      w.language_code, w.project_code FROM agg_page_activity a
      JOIN top_pages t ON a.page_key = t.page_key
      JOIN dim_page p ON a.page_key = p.page_key JOIN dim_wiki w ON a.wiki_key = w.wiki_key
