"""Generate an AI/BI dashboard with filters bound to every relevant dataset."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def dashboard():
    edits = '''SELECT f.event_id, f.event_time, f.event_date, f.bucket_start, f.wiki,
      w.language_code, w.project_code, w.site_name, p.title, e.user_name,
      n.namespace_name, f.editor_key, f.page_key, f.bytes_delta,
      CASE WHEN f.bot THEN 'Bot' ELSE 'Human / unspecified' END AS editor_type
    FROM fact_edits f
    JOIN dim_wiki w ON f.wiki_key = w.wiki_key
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_page p ON f.page_key = p.page_key
    JOIN dim_editor e ON f.editor_key = e.editor_key
    JOIN dim_namespace n ON f.namespace_key = n.namespace_key'''
    five = '''SELECT a.*, CAST(a.bucket_start AS DATE) AS event_date,
      w.language_code, w.project_code FROM agg_wiki_5min a
      JOIN dim_wiki w ON a.wiki_key = w.wiki_key'''
    daily = '''SELECT a.*, w.language_code, w.project_code FROM agg_wiki_daily a
      JOIN dim_wiki w ON a.wiki_key = w.wiki_key'''
    # Rank complete pages before joining daily rows, so the chart has at most 20
    # categories without discarding dates from the retained pages.
    pages = '''WITH top_pages AS (
      SELECT page_key FROM agg_page_activity
      GROUP BY page_key ORDER BY SUM(edit_count) DESC, page_key LIMIT 20
    )
    SELECT a.*, p.title, CONCAT(a.wiki, ': ', p.title) AS page_label,
      w.language_code, w.project_code FROM agg_page_activity a
      JOIN top_pages t ON a.page_key = t.page_key
      JOIN dim_page p ON a.page_key = p.page_key JOIN dim_wiki w ON a.wiki_key = w.wiki_key'''
    queries = {'edits': edits, 'five': five, 'daily': daily, 'pages': pages}
    layout = []

    def widget(name, title, dataset, fields, kind, encodings, x, y, width=6, height=5):
        layout.append({'widget': {'name': name, 'queries': [{'name': 'main', 'query': {
            'datasetName': dataset, 'fields': [{'name': n, 'expression': e} for n,e in fields],
            'disaggregated': False}}], 'spec': {'version': 2 if kind == 'counter' else 3,
                'widgetType': kind, 'frame': {'title': title, 'showTitle': True},
                'encodings': encodings, 'data': {'queryName': 'main'}}},
            'position': {'x': x, 'y': y, 'width': width, 'height': height}})

    for i,(name,title,expression) in enumerate([
        ('total_edits','Captured edits','COUNT(*)'),
        ('total_editors','Observed editors','COUNT(DISTINCT editor_key)'),
        ('total_pages','Edited pages','COUNT(DISTINCT page_key)'),
        ('net_bytes','Net size change (bytes)','SUM(bytes_delta)'),
    ]):
        widget(name,title,'edits',[('value',expression)],'counter',
               {'value':{'fieldName':'value','rowNumber':0}},i*3,0,3,3)

    def axis(name, scale):
        return {'fieldName': name, 'scale': {'type':scale}}

    widget('trend','Captured edits / 5 minutes (UTC)','five',
           [('bucket','bucket_start'),('edits','SUM(edit_count)')],'line',
           {'x':axis('bucket','temporal'),'y':axis('edits','quantitative')},0,3)
    widget('wiki_rank','Activity by wiki','edits',[('wiki','wiki'),('edits','COUNT(*)')],'bar',
           {'x':axis('wiki','categorical'),'y':axis('edits','quantitative')},6,3)
    widget('automation','Bots and human edits','edits',
           [('type','editor_type'),('edits','COUNT(*)')],'pie',
           {'angle':axis('edits','quantitative'),'color':axis('type','categorical')},0,8)
    widget('namespaces','Namespace activity','edits',
           [('namespace','namespace_name'),('edits','COUNT(*)')],'bar',
           {'x':axis('namespace','categorical'),'y':axis('edits','quantitative')},6,8)
    widget('pages','Top 20 pages — captured sample','pages',
           [('page','`page_label`'),('edits','SUM(edit_count)')],'bar',
           {'x':axis('edits','quantitative'),'y':axis('page','categorical')},0,13)
    page_widget = layout[-1]['widget']
    page_widget['queries'][0]['query']['orders'] = [
        {'expression': 'SUM(edit_count)', 'direction': 'DESC'},
        {'expression': '`page_label`', 'direction': 'ASC'},
    ]
    page_widget['spec']['frame'].update({
        'description': 'Top 20 across the captured sample. Filters narrow this list.',
        'showDescription': True,
    })
    widget('daily','Captured edits by day — partial coverage','daily',
           [('date','event_date'),('edits','SUM(edit_count)')],'bar',
           {'x':axis('date','temporal'),'y':axis('edits','quantitative')},6,13)
    filters = []
    for i,(field,title,kind) in enumerate([
        ('event_date','Date (UTC)','filter-date-range-picker'),
        ('wiki','Wiki','filter-multi-select'),
        ('language_code','Language','filter-multi-select'),
        ('project_code','Project','filter-multi-select'),
    ]):
        qs, bindings = [], []
        for dataset in queries:
            name = dataset + '_' + field
            qs.append({'name':name,'query':{'datasetName':dataset,
                'fields':[{'name':field,'expression':'`'+field+'`'}], 'disaggregated':False}})
            bindings.append({'fieldName':field,'queryName':name})
        filters.append({'widget':{'name':'filter_'+field,'queries':qs,'spec':{
            'version':2,'widgetType':kind,'frame':{'title':title,'showTitle':True},
            'encodings':{'fields':bindings}}},
            'position':{'x':i*3,'y':0,'width':3,'height':2}})
    return {'datasets':[{'name':name,'displayName':name,'queryLines':sql.splitlines(keepends=True)}
                        for name,sql in queries.items()],
            'pages':[{'name':'overview','displayName':'Wikimedia activity — captured sample',
                      'layout':layout,'pageType':'PAGE_TYPE_CANVAS','layoutVersion':'GRID_V1'},
                     {'name':'filters','displayName':'Filters','layout':filters,
                      'pageType':'PAGE_TYPE_GLOBAL_FILTERS','layoutVersion':'GRID_V1'}]}


if __name__ == '__main__':
    output = ROOT / 'dashboards' / 'wikimedia_activity.lvdash.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dashboard(), indent=2) + '\n')
    print(output)
