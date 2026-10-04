"""Fetch public Wikimedia references; snapshots can be loaded from a UC Volume."""
import json
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

USER_AGENT = "DatabricksLab6/1.0 (educational Wikimedia analytics)"
SITEMATRIX_URL = "https://en.wikipedia.org/w/api.php"


def get_json(url, params):
    request = Request(url + "?" + urlencode(params), headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urlopen(request, timeout=25) as response:
                result = json.load(response)
            break
        except HTTPError as exc:
            if exc.code not in (429, 503) or attempt == 2:
                raise
            retry_after = exc.headers.get('Retry-After', '')
            delay = float(retry_after) if retry_after.isdigit() else 2 ** (attempt + 1)
            time.sleep(min(delay, 30))
    if "error" in result:
        raise RuntimeError(result["error"])
    return result


def flatten_matrix(matrix):
    rows = []
    for key, value in matrix.items():
        if key == "specials":
            sites, language, language_name = value, None, "Multilingual / special"
        elif isinstance(value, dict) and "site" in value:
            sites, language = value["site"], value.get("code")
            language_name = value.get("localname") or value.get("name") or language
        else:
            continue
        for site in sites:
            rows.append({
                "wiki": site["dbname"], "language_code": language,
                "language_name": language_name, "project_code": site.get("code"),
                "site_name": site.get("sitename"), "site_url": site.get("url"),
                "is_closed": "closed" in site, "is_private": "private" in site,
                "raw_json": json.dumps(site, ensure_ascii=False),
            })
    return rows


def fetch_snapshot(observed_wikis):
    matrix = {}
    continuation = {}
    while True:
        payload = get_json(SITEMATRIX_URL, {
            "action": "sitematrix", "format": "json", "smlimit": "max", **continuation,
        })
        matrix.update(payload["sitematrix"])
        continuation = payload.get("continue")
        if not continuation:
            break
    sites = flatten_matrix(matrix)
    namespaces, errors = {}, {}
    for site in sites:
        wiki = site["wiki"]
        if wiki not in observed_wikis or site["is_private"] or site["is_closed"]:
            continue
        url = urlparse(site["site_url"] or "")
        # Only public Wikimedia domains from SiteMatrix are fetched.
        allowed = ("wikipedia.org", "wiktionary.org", "wikimedia.org", "wikidata.org",
                   "wikisource.org", "wikibooks.org", "wikiquote.org", "wikinews.org",
                   "wikiversity.org", "wikivoyage.org", "mediawiki.org", "wikifunctions.org")
        if url.scheme != "https" or not any(url.hostname == d or (url.hostname or "").endswith("." + d) for d in allowed):
            errors[wiki] = "Unsupported public domain"
            continue
        try:
            result = get_json(site["site_url"] + "/w/api.php", {
                "action": "query", "meta": "siteinfo", "siprop": "namespaces", "format": "json",
            })
            namespaces[wiki] = result["query"]["namespaces"]
        except Exception as exc:
            # Preserve events; namespace labels fall back to the numeric ID.
            errors[wiki] = str(exc)
    return {"sites": sites, "namespaces": namespaces, "namespace_errors": errors,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat()}
