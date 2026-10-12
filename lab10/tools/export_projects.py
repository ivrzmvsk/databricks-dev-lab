"""Export public project metadata from the explicitly selected LAB 9 environment."""

import argparse
import json

from common import ROOT, TARGETS, sql_client

parser = argparse.ArgumentParser()
parser.add_argument("--target", choices=TARGETS, default="personal")
args = parser.parse_args()
sql = sql_client(args.target)
if args.target == "personal":
    rows = sql("""
        SELECT r.wiki AS wiki_id, r.language_code, r.language_name, r.project_code,
               r.site_name AS project_name
        FROM workspace.dev_ivanrazumovskyi_ivanrazumovskyi_lab9_gold.dim_wiki r
        JOIN (SELECT DISTINCT wiki FROM workspace.dev_ivanrazumovskyi_ivanrazumovskyi_lab9_silver.wiki_silver_lab5) f
          ON r.wiki = f.wiki
        ORDER BY r.wiki
    """)
    filename = "wiki_projects.json"
else:
    rows = sql("""
        SELECT DISTINCT wiki AS wiki_id, language_code, language_name, project_code,
               site_name AS project_name
        FROM dbr_dev.ivanrazumovskyi_lab9_gold.dim_wiki
        WHERE wiki IN (SELECT DISTINCT wiki FROM dbr_dev.ivanrazumovskyi_lab9_gold.fact_edits)
        ORDER BY wiki_id
    """)
    baseline = json.loads((ROOT / "data/wiki_projects.json").read_text())
    baseline_keys = {row["wiki_id"] for row in baseline}
    rows = [row for row in rows if row["wiki_id"] not in baseline_keys]
    filename = "wiki_projects_trial_additions.json"
if not rows or len({r["wiki_id"] for r in rows}) != len(rows):
    raise RuntimeError("Exported projects must be nonempty and unique")
path = ROOT / "data" / filename
path.parent.mkdir(exist_ok=True)
path.write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n")
print(json.dumps({"exported_projects": len(rows), "file": str(path)}))
