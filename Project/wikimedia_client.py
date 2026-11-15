
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import requests

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_ARTICLE_PREFIX = "https://en.wikipedia.org/wiki/"

PLOT_HEADINGS = ("plot", "synopsis", "plot summary", "summary", "premise", "story", "storyline")


def default_user_agent():
    return f"ECE-BigData-project by https://github.com/albengs)"


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def article_url(title):
    return WIKIPEDIA_ARTICLE_PREFIX + quote(title.replace(" ", "_"), safe="()_,'!-")


def revision_url(title, revid):
    return f"https://en.wikipedia.org/w/index.php?title={quote(title.replace(' ', '_'), safe='()_,')}&oldid={revid}"


def chunks(items, size):
    for start in range(0, len(items), size):
        yield items[start:start + size]


class RateLimitedSession:
    """HTTP session that waits `min_interval` seconds between requests and retries politely."""

    def __init__(self, min_interval=1.0, max_retries=5, timeout=60, user_agent=None):
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent or default_user_agent(), "Accept": "application/json"})
        self._last_request = 0.0
        self.request_count = 0

    def _wait(self):
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)

    def request(self, method, url, allow_not_found=False, **kwargs):
        for attempt in range(self.max_retries):
            self._wait()
            self._last_request = time.monotonic()
            self.request_count += 1
            try:
                response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            except requests.RequestException:
                time.sleep(2 ** attempt * 5)
                continue
            retry_after = float(response.headers.get("Retry-After", 2 ** attempt * 5))
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(retry_after)
                continue
            if allow_not_found and response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and data.get("error", {}).get("code") == "maxlag":
                time.sleep(retry_after)
                continue
            return data
        raise RuntimeError(f"Giving up on {url} after {self.max_retries} attempts")

    def mediawiki_query(self, params):
        """Run an action=query request and follow `continue` until every page is complete."""
        params = dict(params)
        merged = {"normalized": [], "redirects": [], "pages": {}}
        while True:
            data = self.request("GET", WIKIPEDIA_API_URL, params=params)
            query = data.get("query", {})
            merged["normalized"] += query.get("normalized", [])
            merged["redirects"] += query.get("redirects", [])
            for page in query.get("pages", []):
                current = merged["pages"].setdefault(page["title"], {})
                for field, value in page.items():
                    if value not in (None, [], "") or field not in current:
                        current[field] = value
            if "continue" not in data:
                break
            params.update(data["continue"])
        return {"query": {**merged, "pages": list(merged["pages"].values())}}


class JsonlStore:
    """Append-only JSON Lines file that remembers which keys were already fetched (resumable runs)."""

    def __init__(self, path, key):
        self.path = Path(path)
        self.key = key
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.done = set()
        if self.path.exists():
            with self.path.open(encoding="utf-8") as handle:
                for line in handle:
                    try:
                        self.done.add(json.loads(line)[key])
                    except (ValueError, KeyError):
                        continue

    def missing(self, keys):
        return sorted({k for k in keys if k and k not in self.done})

    def append(self, records):
        with self.path.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                self.done.add(record[self.key])


def wikidata_by_imdb_ids(client, imdb_ids, with_person_facts=False):
    """Map IMDb IDs to Wikidata items and English Wikipedia articles (Wikidata property P345)."""
    values = " ".join(f'"{i}"' for i in imdb_ids)
    person_fields = """
      OPTIONAL { ?item wdt:P569 ?birthDate . }
      OPTIONAL { ?item wdt:P570 ?deathDate . }
      OPTIONAL { ?item wdt:P19 ?birthPlace . ?birthPlace rdfs:label ?birthPlaceLabel . FILTER(LANG(?birthPlaceLabel) = "en") }
    """ if with_person_facts else ""
    query = f"""
    SELECT ?imdb ?item ?article ?description ?birthDate ?deathDate ?birthPlaceLabel WHERE {{
      VALUES ?imdb {{ {values} }}
      ?item wdt:P345 ?imdb .
      OPTIONAL {{ ?article schema:about ?item ; schema:isPartOf <https://en.wikipedia.org/> . }}
      OPTIONAL {{ ?item schema:description ?description . FILTER(LANG(?description) = "en") }}
      {person_fields}
    }}
    """
    data = client.request("POST", WIKIDATA_SPARQL_URL, data={"query": query, "format": "json"},
                          headers={"Accept": "application/sparql-results+json"})
    retrieved_at = utc_now()
    found = {}
    for row in data["results"]["bindings"]:
        imdb_id = row["imdb"]["value"]
        found.setdefault(imdb_id, []).append({
            "imdb_id": imdb_id,
            "wikidata_id": row["item"]["value"].rsplit("/", 1)[-1],
            "wikidata_url": row["item"]["value"],
            "wikipedia_url": row.get("article", {}).get("value"),
            "wikidata_description": row.get("description", {}).get("value"),
            "birth_date": row.get("birthDate", {}).get("value"),
            "death_date": row.get("deathDate", {}).get("value"),
            "birth_place": row.get("birthPlaceLabel", {}).get("value"),
            "retrieved_at": retrieved_at,
            "source": "wikidata",
            "license": "CC0 1.0",
        })
    records = []
    for imdb_id in imdb_ids:
        matches = found.get(imdb_id) or [{
            "imdb_id": imdb_id, "wikidata_id": None, "wikidata_url": None, "wikipedia_url": None,
            "wikidata_description": None, "birth_date": None, "death_date": None, "birth_place": None,
            "retrieved_at": retrieved_at, "source": "wikidata", "license": "CC0 1.0",
        }]
        distinct_items = len({m["wikidata_id"] for m in found.get(imdb_id, [])})
        records.extend({**m, "matches": distinct_items} for m in matches)
    return records


def title_from_url(url):
    from urllib.parse import unquote
    return unquote(url[len(WIKIPEDIA_ARTICLE_PREFIX):]).replace("_", " ")


def split_sections(wikitext):
    """Return (lead_wikitext, plot_heading, plot_wikitext) from an article's raw wikitext."""
    headings = list(re.finditer(r"^==\s*([^=].*?)\s*==\s*$", wikitext, flags=re.MULTILINE))
    lead = wikitext[: headings[0].start()] if headings else wikitext
    for index, heading in enumerate(headings):
        name = re.sub(r"<[^>]+>|\[\[|\]\]|'''|''", "", heading.group(1)).strip().lower()
        if name in PLOT_HEADINGS:
            end = headings[index + 1].start() if index + 1 < len(headings) else len(wikitext)
            return lead, heading.group(1).strip(), wikitext[heading.end():end]
    return lead, None, None


def wikipedia_film_pages(client, wikipedia_urls):
    """Fetch the latest revision wikitext of film articles and keep the lead and plot sections (raw)."""
    titles = [title_from_url(u) for u in wikipedia_urls]
    data = client.mediawiki_query({
        "action": "query", "format": "json", "formatversion": "2", "redirects": "1", "maxlag": "5",
        "prop": "revisions", "rvprop": "ids|content", "rvslots": "main", "titles": "|".join(titles),
    })
    return _page_records(data, wikipedia_urls, titles, kind="film")


def wikipedia_person_intros(client, wikipedia_urls):
    """Fetch the plain-text introduction of person articles (TextExtracts) with their revision id."""
    titles = [title_from_url(u) for u in wikipedia_urls]
    data = client.mediawiki_query({
        "action": "query", "format": "json", "formatversion": "2", "redirects": "1", "maxlag": "5",
        "prop": "extracts|revisions", "rvprop": "ids", "exintro": "1", "explaintext": "1",
        "exlimit": "max", "titles": "|".join(titles),
    })
    return _page_records(data, wikipedia_urls, titles, kind="person")


def _page_records(data, wikipedia_urls, titles, kind):
    retrieved_at = utc_now()
    query = data.get("query", {})
    renamed = {}
    for step in query.get("normalized", []) + query.get("redirects", []):
        renamed[step["from"]] = step["to"]
    pages = {page["title"]: page for page in query.get("pages", [])}
    records = []
    for url, requested in zip(wikipedia_urls, titles):
        title = requested
        while title in renamed:
            title = renamed[title]
        page = pages.get(title, {})
        revision = (page.get("revisions") or [{}])[0]
        record = {
            "wikipedia_url": url,
            "resolved_title": page.get("title"),
            "page_id": page.get("pageid"),
            "revision_id": revision.get("revid"),
            "revision_url": revision_url(page["title"], revision["revid"]) if revision.get("revid") else None,
            "missing": bool(page.get("missing")) or not page,
            "retrieved_at": retrieved_at,
            "source": "wikipedia",
            "license": "CC BY-SA 4.0",
        }
        if kind == "film":
            content = revision.get("slots", {}).get("main", {}).get("content") or ""
            lead, plot_heading, plot = split_sections(content) if content else (None, None, None)
            record.update({"lead_wikitext": lead, "plot_heading": plot_heading, "plot_wikitext": plot})
        else:
            record.update({"intro_text": page.get("extract")})
        records.append(record)
    return records


PAGEVIEWS_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"


def pageviews_monthly(client, wikipedia_urls, start="20150701", end=None):
    """Monthly human page views of English Wikipedia articles (Wikimedia Pageviews API, data from July 2015).

    One request per article. `agent=user` excludes known bots and crawlers. Returns one record per
    article and month; an article without data gets a single record with `month = None`.
    """
    if end is None:
        first_of_month = datetime.now(timezone.utc).replace(day=1)
        end = (first_of_month - timedelta(days=1)).strftime("%Y%m%d")
    records = []
    for url in wikipedia_urls:
        title = title_from_url(url)
        source = (f"{PAGEVIEWS_URL}/en.wikipedia/all-access/user/"
                  f"{quote(title.replace(' ', '_'), safe='')}/monthly/{start}00/{end}00")
        data = client.request("GET", source, allow_not_found=True)
        retrieved_at = utc_now()
        items = (data or {}).get("items", [])
        base = {"wikipedia_url": url, "source_url": source, "retrieved_at": retrieved_at,
                "source": "wikimedia_pageviews", "license": "CC0 1.0"}
        if not items:
            records.append({**base, "month": None, "views": None})
        for item in items:
            records.append({**base, "month": item["timestamp"][:6], "views": item["views"]})
    return records


def wikipedia_article_info(client, wikipedia_urls):
    """Current size in bytes and latest revision of English Wikipedia articles (50 titles per request)."""
    titles = [title_from_url(u) for u in wikipedia_urls]
    data = client.mediawiki_query({
        "action": "query", "format": "json", "formatversion": "2", "redirects": "1", "maxlag": "5",
        "prop": "info", "titles": "|".join(titles),
    })
    query = data["query"]
    renamed = {step["from"]: step["to"] for step in query.get("normalized", []) + query.get("redirects", [])}
    pages = {page["title"]: page for page in query.get("pages", [])}
    retrieved_at = utc_now()
    records = []
    for url, title in zip(wikipedia_urls, titles):
        while title in renamed:
            title = renamed[title]
        page = pages.get(title, {})
        records.append({
            "wikipedia_url": url, "resolved_title": page.get("title"),
            "article_bytes": page.get("length"), "last_revision_id": page.get("lastrevid"),
            "source_url": revision_url(page["title"], page["lastrevid"]) if page.get("lastrevid") else None,
            "retrieved_at": retrieved_at, "source": "wikipedia", "license": "CC BY-SA 4.0",
        })
    return records


def wikidata_sitelinks(client, wikidata_ids):
    """Number of Wikimedia sites (mostly Wikipedia language editions) with a page about each Wikidata item."""
    values = " ".join(f"wd:{q}" for q in wikidata_ids)
    query = f"SELECT ?item ?sitelinks WHERE {{ VALUES ?item {{ {values} }} ?item wikibase:sitelinks ?sitelinks . }}"
    data = client.request("POST", WIKIDATA_SPARQL_URL, data={"query": query, "format": "json"},
                          headers={"Accept": "application/sparql-results+json"})
    retrieved_at = utc_now()
    found = {row["item"]["value"].rsplit("/", 1)[-1]: int(row["sitelinks"]["value"])
             for row in data["results"]["bindings"]}
    return [{
        "wikidata_id": q, "sitelinks": found.get(q),
        "source_url": f"https://www.wikidata.org/wiki/{q}", "retrieved_at": retrieved_at,
        "source": "wikidata", "license": "CC0 1.0",
    } for q in wikidata_ids]


def collect(store, keys, fetch, batch_size, label, progress_every=10):
    """Fetch every key not yet in `store`, in batches, and append the results. Returns the number fetched."""
    todo = store.missing(keys)
    print(f"{label}: {len(keys):,} requested, {len(keys) - len(todo):,} already cached, {len(todo):,} to fetch")
    for number, batch in enumerate(chunks(todo, batch_size), start=1):
        store.append(fetch(batch))
        if number % progress_every == 0:
            print(f"  {label}: {min(number * batch_size, len(todo)):,}/{len(todo):,}")
    return len(todo)
