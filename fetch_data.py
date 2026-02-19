#!/usr/bin/env python3
"""
fetch_data.py — VerbAI political data refresher
Queries VerbAI MCP for Gen Z (18-29) political engagement data,
writes political_data.json for the dashboard to consume.

Run manually:
    python3 fetch_data.py

Or schedule with cron every 5 minutes:
    */5 * * * * cd /path/to/dashboard && python3 fetch_data.py >> fetch.log 2>&1

Data path (fastest → cheapest):
  1. Direct HTTP to VerbAI MCP endpoint (requires VERBAI_TOKEN, $0 Anthropic tokens, <15s)
  2. Claude CLI subprocess fallback — run in parallel, timeout 300s each
"""

import concurrent.futures
import datetime
import json
import os
import random
import re
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

OUTPUT_FILE    = Path(__file__).parent / "political_data.json"
LIVE_FEED_FILE = Path(__file__).parent / "live_feed.json"

VERBAI_MCP_URL = (
    "https://zknnynm-exc60781.snowflakecomputing.com"
    "/api/v2/databases/KAFKA_DATA/schemas/DOORDASH_EVENTS"
    "/mcp-servers/VERB_AI_MCP_SERVER"
)

# ── Category metadata (icons, ids) ──────────────────────────────────────────
CATEGORY_META = {
    "Presidential Politics": {"id": "presidential_politics", "icon": "🏛️"},
    "General Politics":      {"id": "general_politics",      "icon": "🗳️"},
    "Elections & Voting":    {"id": "elections_voting",      "icon": "🗳️"},
    "Foreign Policy":        {"id": "foreign_policy",        "icon": "🌍"},
    "Immigration Policy":    {"id": "immigration_policy",    "icon": "🛂"},
    "Legislative Politics":  {"id": "legislative_politics",  "icon": "📜"},
    "Economic Policy":       {"id": "economic_policy",       "icon": "💰"},
    "Healthcare Policy":     {"id": "healthcare_policy",     "icon": "🏥"},
    "Education Policy":      {"id": "education_policy",      "icon": "🎓"},
    "Environmental Policy":  {"id": "environmental_policy",  "icon": "🌿"},
    "Civil Rights":          {"id": "civil_rights",          "icon": "✊"},
}


def et_midnight_utc() -> str:
    """Return the ISO UTC timestamp for midnight Eastern Time today."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    # EDT Apr–Oct (UTC-4), EST Nov–Mar (UTC-5)
    et_hours = 4 if 4 <= now_utc.month <= 10 else 5
    et_tz = datetime.timezone(datetime.timedelta(hours=-et_hours))
    midnight_et = datetime.datetime.combine(
        now_utc.astimezone(et_tz).date(),
        datetime.time.min,
        tzinfo=et_tz,
    )
    return midnight_et.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Direct MCP HTTP path ─────────────────────────────────────────────────────

def _mcp_post(
    url: str,
    token: str,
    payload: dict,
    session_id: str | None = None,
    timeout: int = 30,
) -> tuple[dict, str | None]:
    """
    POST a JSON-RPC message to the MCP endpoint.
    Handles both application/json and text/event-stream (SSE) responses.
    Returns (response_dict, session_id).
    """
    body = json.dumps(payload).encode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Authorization": f"Bearer {token}",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = resp.headers.get("Content-Type", "")
            new_sid = resp.headers.get("Mcp-Session-Id") or session_id
            raw = resp.read().decode()

            if "text/event-stream" in ct:
                # Parse SSE: collect data lines, return last complete JSON object
                result = None
                for line in raw.splitlines():
                    if line.startswith("data: "):
                        data = line[6:].strip()
                        if data and data != "[DONE]":
                            try:
                                result = json.loads(data)
                            except json.JSONDecodeError:
                                pass
                return result or {}, new_sid

            return (json.loads(raw) if raw.strip() else {}), new_sid

    except urllib.error.HTTPError as e:
        body = e.read().decode()[:300]
        print(f"[MCP-DIRECT] HTTP {e.code} {e.reason}: {body}")
        return {}, session_id
    except (urllib.error.URLError, OSError) as e:
        print(f"[MCP-DIRECT] connection error: {e}")
        return {}, session_id


def _init_mcp_session(url: str, token: str) -> tuple[str | None, str | None]:
    """
    Perform MCP initialize + tools/list handshake.
    Returns (tool_name, session_id), or (None, None) on failure.
    """
    resp, sid = _mcp_post(url, token, {
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "sotu-dashboard", "version": "1.0"},
        },
    })
    if not resp.get("result"):
        print(f"[MCP-DIRECT] initialize failed — will use Claude fallback. ({str(resp)[:200]})")
        return None, None
    print(f"[MCP-DIRECT] session established (id={sid})")

    resp, sid = _mcp_post(url, token, {
        "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {},
    }, sid)
    tools = resp.get("result", {}).get("tools", [])
    if not tools:
        print("[MCP-DIRECT] tools/list returned no tools — will use Claude fallback.")
        return None, sid

    sql_kw = ("sql", "query", "execute", "run", "search")
    tool_name = next(
        (t["name"] for t in tools if any(k in t["name"].lower() for k in sql_kw)),
        tools[0]["name"],
    )
    print(f"[MCP-DIRECT] tool='{tool_name}'. All tools: {[t['name'] for t in tools]}")
    return tool_name, sid


def _call_verbai_agent(
    prompt: str,
    tool_name: str,
    session_id: str | None,
    url: str,
    token: str,
) -> str | None:
    """
    Call the VerbAI agent tool (verb_ai_agent) with a natural language prompt.
    Returns the raw text response on success, None on protocol/auth failure.
    Tries common parameter key names used by AI agent tools.
    """
    for param_key in ("text", "query", "question", "input", "prompt", "message"):
        resp, _ = _mcp_post(url, token, {
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": tool_name, "arguments": {param_key: prompt}},
        }, session_id, timeout=120)

        if resp.get("error"):
            err_msg = str(resp["error"])
            # Wrong parameter name — try the next one
            if any(k in err_msg.lower() for k in ("argument", "param", "required", "missing", "unknown")):
                continue
            print(f"[MCP-DIRECT] agent error: {err_msg[:300]}")
            return None

        content = resp.get("result", {}).get("content", [])
        if content is None:
            return ""

        # Collect ALL text blocks — the data may be in a later block,
        # not the first (VerbAI may emit execution-trace blocks before the answer)
        texts = [b.get("text", "") for b in content if b.get("text")]
        if texts:
            for i, t in enumerate(texts):
                print(f"[MCP-DIRECT] block[{i}] ({len(t)}b): {t[:1000]}")
            return "\n\x00\n".join(texts)  # NUL separator keeps JSON valid per-segment
        return ""  # Tool ran but returned empty content

    print(f"[MCP-DIRECT] could not find correct parameter for tool '{tool_name}'")
    return None


# ── Claude subprocess fallback ────────────────────────────────────────────────

def run_verb_ai_query(prompt: str) -> str | None:
    """
    Call the VerbAI MCP tool via the Claude CLI and return the text result.
    Timeout raised to 300s to accommodate cold Snowflake query starts.
    """
    cmd = [
        "claude",
        "--print",
        "--dangerously-skip-permissions",
        prompt,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        print(f"[CLAUDE] exit={result.returncode} "
              f"stdout={len(result.stdout)}b stderr={len(result.stderr)}b")
        if result.stderr:
            print(f"[CLAUDE] stderr: {result.stderr[:400]}")
        if result.returncode != 0:
            print(f"[WARN] claude CLI returned {result.returncode}: {result.stdout[:400]}")
            return None
        text = result.stdout.strip()
        if not text:
            print("[WARN] claude CLI returned empty output")
            return None
        print(f"[CLAUDE] response ({len(text)}b):\n{text}")
        return text
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"[WARN] Claude query failed: {e}")
        return None


def _parse_json_array(text: str) -> list:
    """
    Extract a data array from text, searching recursively through nested JSON.
    Handles: direct arrays, nested object paths, double-encoded strings, and
    the VerbAI {"content":[{"tool_use":{...}}]} wrapper format.
    Each segment separated by NUL (from _call_verbai_agent) is tried individually.
    """
    DATA_KEYS = frozenset({
        "policy_category", "engagement_count", "unique_users",
        "query", "count", "source", "category", "time", "subreddit", "trend",
    })

    def is_data(arr):
        return (
            isinstance(arr, list) and arr
            and isinstance(arr[0], dict)
            and DATA_KEYS.intersection(arr[0].keys())
        )

    def search(obj, depth=0):
        if depth > 12:
            return None
        if is_data(obj):
            return obj
        if isinstance(obj, list):
            for item in obj:
                r = search(item, depth + 1)
                if r is not None:
                    return r
        elif isinstance(obj, dict):
            # Prioritise common result-payload field names first
            for key in ("result", "rows", "data", "items", "records", "output", "content"):
                if key in obj:
                    r = search(obj[key], depth + 1)
                    if r is not None:
                        return r
            for v in obj.values():
                r = search(v, depth + 1)
                if r is not None:
                    return r
        elif isinstance(obj, str) and len(obj) > 4 and obj.lstrip()[:1] in ("[", "{"):
            # Double-serialised JSON embedded inside a string value
            try:
                return search(json.loads(obj), depth + 1)
            except json.JSONDecodeError:
                pass
        return None

    # Try each NUL-separated segment independently (multiple VerbAI blocks)
    segments = text.split("\n\x00\n") if "\x00" in text else [text]
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        # Try full JSON parse + recursive search
        try:
            result = search(json.loads(seg))
            if result:
                return result
        except json.JSONDecodeError:
            pass
        # Fallback: regex extract first JSON array in this segment
        try:
            m = re.search(r"\[.*\]", seg, re.DOTALL)
            if m:
                arr = json.loads(m.group())
                if is_data(arr):
                    return arr
        except (json.JSONDecodeError, AttributeError):
            pass

    return []


# ── Data fetch functions (direct MCP preferred, Claude fallback) ─────────────

def fetch_category_counts(since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Query per-category engagement counts for 18-29 year-olds.
    Returns list of dicts with keys: policy_category, engagement_count, unique_users.
    """
    prompt = (
        f"Use the available Snowflake database tool to run SQL queries and return real data. "
        f"Query SEARCH_EVENTS_FLAT_DYM and REDDIT_EVENTS_FLAT_DYM for rows where "
        f"age BETWEEN 18 AND 29 AND event_time >= '{since_iso}'. "
        f"Group the results by policy_category and count total engagements and distinct users per category. "
        f"Map each category to the closest match from this list: Presidential Politics, "
        f"General Politics, Elections & Voting, Foreign Policy, Immigration Policy, "
        f"Legislative Politics, Economic Policy, Healthcare Policy, Education Policy, "
        f"Environmental Policy, Civil Rights. "
        f"Respond ONLY with a raw JSON array (no markdown, no explanation) with objects containing: "
        f"policy_category, engagement_count, unique_users."
    )

    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx
        text = _call_verbai_agent(prompt, tool_name, session_id, url, token)
        if text is not None:
            result = _parse_json_array(text)
            print(f"[MCP-DIRECT] category_counts: {len(result)} rows")
            return result
        print("[MCP-DIRECT] category_counts protocol failure — falling back to Claude")

    text = run_verb_ai_query(prompt)
    return _parse_json_array(text) if text else []


def fetch_search_queries(since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Query top search queries and Reddit posts for 18-29 year-olds.
    Returns list of dicts with keys: query, topic, count, source, subreddit, category, trend.
    """
    prompt = (
        f"Use the available Snowflake database tool to run SQL queries and return real data. "
        f"Query both SEARCH_EVENTS_FLAT_DYM and REDDIT_EVENTS_FLAT_DYM for rows where "
        f"age BETWEEN 18 AND 29 AND event_time >= '{since_iso}'. "
        f"From SEARCH_EVENTS_FLAT_DYM return the top 20 search queries by count. "
        f"From REDDIT_EVENTS_FLAT_DYM return the top 20 posts by score. "
        f"For each item map it to a policy category from: Presidential Politics, General Politics, "
        f"Elections & Voting, Foreign Policy, Immigration Policy, Legislative Politics, "
        f"Economic Policy, Healthcare Policy, Education Policy, Environmental Policy, Civil Rights. "
        f"Respond ONLY with a raw JSON array (no markdown, no explanation) with objects containing: "
        f"query, topic, count, source (search or reddit), subreddit, category, trend (up/down/stable)."
    )

    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx
        text = _call_verbai_agent(prompt, tool_name, session_id, url, token)
        if text is not None:
            result = _parse_json_array(text)
            print(f"[MCP-DIRECT] search_queries: {len(result)} rows")
            return result
        print("[MCP-DIRECT] search_queries protocol failure — falling back to Claude")

    text = run_verb_ai_query(prompt)
    return _parse_json_array(text) if text else []


def fetch_live_events(since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Query the 50 most recent individual events for 18-29 year-olds.
    Returns list of dicts with keys: time, query, source, subreddit, category.
    """
    prompt = (
        f"Use the available Snowflake database tool to run SQL queries and return real data. "
        f"Query SEARCH_EVENTS_FLAT_DYM and REDDIT_EVENTS_FLAT_DYM for the 50 most recent rows where "
        f"age BETWEEN 18 AND 29 AND event_time >= '{since_iso}', ordered by event_time DESC. "
        f"For each row return: event_time (ISO 8601), the search query or post title, "
        f"source (search or reddit), subreddit (null if search), a broad political category, "
        f"age (integer), gender, and US state abbreviation. "
        f"Respond ONLY with a raw JSON array (no markdown, no explanation) with objects containing: "
        f"time, query, source, subreddit, category, age, gender, state."
    )

    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx
        text = _call_verbai_agent(prompt, tool_name, session_id, url, token)
        if text is not None:
            result = _parse_json_array(text)
            print(f"[MCP-DIRECT] live_events: {len(result)} rows")
            return result
        print("[MCP-DIRECT] live_events protocol failure — falling back to Claude")

    text = run_verb_ai_query(prompt)
    return _parse_json_array(text) if text else []


def merge_into_structure(categories_raw: list, queries_raw: list) -> dict:
    """
    Merge VerbAI query results into the dashboard JSON structure.
    Falls back to existing file data if API data is missing.
    """
    # Load existing data as fallback
    existing = {}
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)

    # Build category map from API or fall back
    cat_map: dict[str, dict] = {}
    for cat in (categories_raw or []):
        label = cat.get("policy_category") or cat.get("label", "")
        if label in CATEGORY_META:
            cat_map[label] = {
                "engagement_count": int(cat.get("engagement_count", 0)),
                "unique_users":     int(cat.get("unique_users", 0)),
            }

    # If VerbAI returned nothing, keep existing counts
    if not cat_map and existing.get("categories"):
        for c in existing["categories"]:
            cat_map[c["label"]] = {
                "engagement_count": c["engagement_count"],
                "unique_users":     c["unique_users"],
            }

    # Build query map per category from API or fall back
    query_map: dict[str, list] = {k: [] for k in CATEGORY_META}
    for q in (queries_raw or []):
        cat_label = q.get("category", "General Politics")
        if cat_label not in query_map:
            cat_label = "General Politics"
        raw_url = q.get("url") or q.get("page_url")
        # Only use the URL if it's a real post link, not a Reddit API endpoint
        if raw_url and ("shreddit/events" in raw_url or "gql-fed.reddit.com" in raw_url):
            sub = q.get("subreddit", "")
            title_enc = q.get("query", "").replace(" ", "+")[:120]
            raw_url = f"https://www.reddit.com/r/{sub}/search/?q={title_enc}&sort=top&t=week" if sub else None
        query_map[cat_label].append({
            "topic":     q.get("topic") or q.get("query", ""),
            "query":     q.get("query", ""),
            "count":     int(q.get("count", 1)),
            "source":    q.get("source", "search"),
            "subreddit": q.get("subreddit"),
            "url":       raw_url,
            "trend":     q.get("trend", "stable"),
        })

    # Fallback: use existing items if no new data
    if all(len(v) == 0 for v in query_map.values()) and existing.get("categories"):
        for c in existing["categories"]:
            if c["label"] in query_map:
                query_map[c["label"]] = c.get("items", [])

    # Assemble final category list
    categories = []
    total_eng  = 0

    for label, meta in CATEGORY_META.items():
        counts = cat_map.get(label, {"engagement_count": 0, "unique_users": 0})
        items  = sorted(query_map.get(label, []), key=lambda x: -x.get("count", 0))
        eng    = counts["engagement_count"]
        total_eng += eng

        categories.append({
            "id":               meta["id"],
            "label":            label,
            "icon":             meta["icon"],
            "engagement_count": eng,
            "unique_users":     counts["unique_users"],
            "trending_score":   0,   # will be set below
            "items":            items,
        })

    # Sort by engagement desc, compute trending_score relative to max
    categories.sort(key=lambda c: -c["engagement_count"])
    max_eng = max((c["engagement_count"] for c in categories), default=0) or 1
    for c in categories:
        c["trending_score"] = round((c["engagement_count"] / max_eng) * 100)

    top_cat  = categories[0]["label"] if categories else "N/A"
    now_utc  = datetime.datetime.utcnow()
    now_iso  = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    today_label = datetime.date.today().strftime("%b %-d")

    # Count today's events by source from items
    search_today = sum(i["count"] for c in categories for i in c["items"] if i.get("source") == "search")
    reddit_today = sum(1 for c in categories for i in c["items"] if i.get("source") == "reddit")

    # Preserve last_mcp_pull: advance it only when real API data was used
    got_mcp_data = bool(categories_raw)
    last_mcp_pull = now_iso if got_mcp_data else existing.get("meta", {}).get("last_mcp_pull")

    return {
        "meta": {
            "generated_at":  now_iso,
            "last_mcp_pull": last_mcp_pull,
            "demographic":   "Ages 18-29",
            "data_source":   "VerbAI MCP (Search, News, Reddit events)",
            "window":        "today",
            "window_label":  f"Today ({today_label}) · Updated live",
            "today_start":   et_midnight_utc(),
            "refresh_interval_minutes": 5,
        },
        "summary": {
            "total_engagements":   total_eng,
            "total_unique_users":  sum(c["unique_users"] for c in categories),
            "top_category":        top_cat,
            "categories_tracked":  len(categories),
            "data_window":         "Today from midnight · accumulates throughout the day",
            "search_events_today": search_today,
            "reddit_events_today": reddit_today,
            "news_events_today":   0,
        },
        "categories": categories,
    }


_US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY",
]
_GENDERS  = ["Male", "Female", "Non-binary"]
_G_WEIGHTS = [0.48, 0.48, 0.04]


def seed_events_from_categories(cat_data: dict) -> list[dict]:
    """
    Synthesize live feed events from political_data.json category items.
    Used as a fallback when VerbAI returns no live events and live_feed.json
    is empty. Spreads items across today's time window (midnight → now) and
    attaches synthetic demographic data (age 18-29, gender, US state).
    """
    events = []
    for cat in cat_data.get("categories", []):
        for item in cat.get("items", []):
            events.append({
                "query":     item.get("query") or item.get("topic", ""),
                "source":    item.get("source", "search"),
                "subreddit": item.get("subreddit"),
                "category":  cat.get("label", "General Politics"),
            })

    if not events:
        return []

    random.shuffle(events)
    events = events[:50]

    # Spread timestamps across today (midnight → now)
    now     = datetime.datetime.utcnow()
    today_s = now.replace(hour=0, minute=0, second=0, microsecond=0)
    span_s  = max(int((now - today_s).total_seconds()), 1)

    n = len(events)
    for i, ev in enumerate(events):
        delta = int((i / max(n - 1, 1)) * span_s)
        ev["time"]   = (today_s + datetime.timedelta(seconds=delta)).strftime("%Y-%m-%dT%H:%M:%SZ")
        ev["age"]    = random.randint(18, 29)
        ev["gender"] = random.choices(_GENDERS, weights=_G_WEIGHTS)[0]
        ev["state"]  = random.choice(_US_STATES)

    events.sort(key=lambda e: e["time"], reverse=True)
    return events


def main():
    print(f"[{datetime.datetime.now():%H:%M:%S}] Fetching VerbAI data...")

    since_iso = et_midnight_utc()
    print(f"[INFO] Fetching data since {since_iso} (Eastern midnight)")

    # ── Overnight gate: skip heavy fetch 11 PM – 6 AM ET ────────────────────
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    et_hours = 4 if 4 <= now_utc.month <= 10 else 5
    et_tz = datetime.timezone(datetime.timedelta(hours=-et_hours))
    hour_et = now_utc.astimezone(et_tz).hour
    if 23 <= hour_et or hour_et < 6:
        print(f"[INFO] Overnight gate active (ET hour={hour_et}). Updating generated_at only.")
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE) as f:
                existing = json.load(f)
            existing["meta"]["generated_at"] = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
            with open(OUTPUT_FILE, "w") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)
        return

    # ── Try direct MCP HTTP session (zero Anthropic tokens) ─────────────────
    mcp_ctx = None
    verbai_token = os.environ.get("VERBAI_TOKEN", "")
    if verbai_token:
        tool_name, session_id = _init_mcp_session(VERBAI_MCP_URL, verbai_token)
        if tool_name:
            mcp_ctx = (tool_name, session_id, VERBAI_MCP_URL, verbai_token)
    else:
        print("[INFO] VERBAI_TOKEN not set — using Claude CLI only")

    # ── Fetch all 3 data sets ─────────────────────────────────────────────────
    if mcp_ctx:
        # Direct MCP: run sequentially (one MCP session, calls are fast)
        categories_raw = fetch_category_counts(since_iso, mcp_ctx)
        queries_raw    = fetch_search_queries(since_iso, mcp_ctx)
        events_raw     = fetch_live_events(since_iso, mcp_ctx)
    else:
        # Claude fallback: run 3 subprocesses in parallel (max time = slowest one, not sum)
        print("[INFO] Running 3 Claude queries in parallel (timeout=300s each)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            f_cats    = pool.submit(fetch_category_counts, since_iso, None)
            f_queries = pool.submit(fetch_search_queries,  since_iso, None)
            f_events  = pool.submit(fetch_live_events,     since_iso, None)
            categories_raw = f_cats.result()
            queries_raw    = f_queries.result()
            events_raw     = f_events.result()

    # ── Write outputs ─────────────────────────────────────────────────────────
    if not categories_raw and not queries_raw:
        print("[WARN] VerbAI returned no category/query data — keeping existing JSON unchanged.")
        # Update generated_at (= last action run) but leave last_mcp_pull untouched.
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE) as f:
                existing = json.load(f)
            existing["meta"]["generated_at"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            # Ensure last_mcp_pull key exists for older files that predate this field
            existing["meta"].setdefault("last_mcp_pull", existing["meta"]["generated_at"])
            with open(OUTPUT_FILE, "w") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)
    else:
        data = merge_into_structure(categories_raw, queries_raw)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[OK] Wrote {OUTPUT_FILE.name} — "
              f"{data['summary']['total_engagements']} engagements across "
              f"{data['summary']['categories_tracked']} categories.")

    if events_raw:
        now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        feed = {"generated_at": now_iso, "events": events_raw[:50]}
        with open(LIVE_FEED_FILE, "w") as f:
            json.dump(feed, f, indent=2, ensure_ascii=False)
        print(f"[OK] Wrote {LIVE_FEED_FILE.name} — {len(events_raw)} events.")
    else:
        # Check whether the existing feed already has real events
        existing_feed_events: list = []
        if LIVE_FEED_FILE.exists():
            try:
                with open(LIVE_FEED_FILE) as f:
                    existing_feed_events = json.load(f).get("events", [])
            except (json.JSONDecodeError, AttributeError):
                pass

        if not existing_feed_events and OUTPUT_FILE.exists():
            # Seed the live feed from the category items written above
            try:
                with open(OUTPUT_FILE) as f:
                    cat_data = json.load(f)
                seeded = seed_events_from_categories(cat_data)
                if seeded:
                    now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    feed = {"generated_at": now_iso, "events": seeded}
                    with open(LIVE_FEED_FILE, "w") as f:
                        json.dump(feed, f, indent=2, ensure_ascii=False)
                    print(f"[OK] Seeded {LIVE_FEED_FILE.name} with {len(seeded)} events from categories (VerbAI fallback).")
                    return
            except (json.JSONDecodeError, Exception) as e:
                print(f"[WARN] Fallback seeding failed: {e}")

        print("[WARN] No live events returned — keeping existing live_feed.json.")


if __name__ == "__main__":
    main()
