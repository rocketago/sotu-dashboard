#!/usr/bin/env python3
"""
fetch_data.py — VerbAI political data refresher
Queries VerbAI MCP for Gen Z (18-29) political engagement data,
writes political_data.json for the dashboard to consume.

Run manually:
    python3 fetch_data.py

Or schedule with cron every 15 minutes:
    */15 * * * * cd /path/to/dashboard && python3 fetch_data.py >> fetch.log 2>&1

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
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

OUTPUT_FILE          = Path(__file__).parent / "political_data.json"
LIVE_FEED_FILE       = Path(__file__).parent / "live_feed.json"
HISTORY_FILE         = Path(__file__).parent / "history.json"
MCP_STATUS_FILE      = Path(__file__).parent / "mcp_status.json"
SOURCES_CACHE_FILE   = Path(__file__).parent / "sources_cache.json"
POLLING_DATA_FILE    = Path(__file__).parent / "polling_data.json"

VERBAI_MCP_URL = (
    "https://zknnynm-exc60781.snowflakecomputing.com"
    "/api/v2/databases/KAFKA_DATA/schemas/DOORDASH_EVENTS"
    "/mcp-servers/VERB_AI_MCP_SERVER"
)

# Rolling 24-hour window anchor.  Each cron run starts the window at the later of
# this value and (now_ET − 24h), so the dashboard always shows the last 24 hours.
# This value is intentionally set to a date well in the past so the window always
# slides with the clock rather than being pinned to any specific event.
WINDOW_ANCHOR = "2026-01-20T00:00:00"   # Inauguration day — window rolls from here

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

# ── Shared SQL keyword blocks for all VerbAI prompts ─────────────────────────
# These focus queries on broad U.S. political topics consumed by young voters.
# Kept in one place so fetch_* functions stay in sync with _is_political_item.

# Topics that should be EXCLUDED even if they match an include keyword
_SQL_EXCLUDE_BLOCK = (
    "AND col NOT ILIKE '%prince andrew%' "
    "AND col NOT ILIKE '%prince william%' "
    "AND col NOT ILIKE '%royal family%' "
    "AND col NOT ILIKE '%south korea%' "
    "AND col NOT ILIKE '%yoon suk%' "
    "AND col NOT ILIKE '%epstein%' "
    "AND col NOT ILIKE '%blondie in china%' "
    "AND col NOT ILIKE '%white girl in china%' "
)

# ILIKE include conditions for search / YouTube / Reddit title columns.
# Covers the breadth of U.S. political topics relevant to voters aged 18-29.
# Placeholder "col" is replaced with the real column alias in each prompt.
_SQL_INCLUDE_BLOCK = (
    # Presidency / executive branch
    "(col ILIKE '%trump%' OR col ILIKE '%white house%' OR col ILIKE '%maga%' "
    "OR col ILIKE '%executive order%' OR col ILIKE '%presidential%' "
    # Congress / legislation
    "OR col ILIKE '%congress%' OR col ILIKE '%senate%' OR col ILIKE '%democrat%' "
    "OR col ILIKE '%republican%' OR col ILIKE '%legislation%' OR col ILIKE '%midterm%' "
    "OR col ILIKE '%election%' OR col ILIKE '%voting rights%' OR col ILIKE '%gerrymandering%' "
    # DOGE / federal workforce
    "OR col ILIKE '%doge%' OR col ILIKE '%elon musk%' "
    "OR col ILIKE '%federal worker%' OR col ILIKE '%federal employee%' "
    "OR col ILIKE '%federal budget%' OR col ILIKE '%spending cut%' "
    # Economy — youth-relevant
    "OR col ILIKE '%tariff%' OR col ILIKE '%trade war%' "
    "OR col ILIKE '%inflation%' OR col ILIKE '%unemployment%' OR col ILIKE '%minimum wage%' "
    "OR col ILIKE '%housing%' OR col ILIKE '%rent%' OR col ILIKE '%housing affordability%' "
    "OR col ILIKE '%student debt%' OR col ILIKE '%college%' OR col ILIKE '%tuition%' "
    "OR col ILIKE '%deficit%' OR col ILIKE '%debt ceiling%' "
    # Immigration
    "OR col ILIKE '%immigration%' OR col ILIKE '%deportation%' "
    "OR col ILIKE '%ice raid%' OR col ILIKE '%daca%' OR col ILIKE '%migrant%' "
    # Foreign policy
    "OR col ILIKE '%ukraine%' OR col ILIKE '%russia%' OR col ILIKE '%nato%' "
    "OR col ILIKE '%china%' OR col ILIKE '%iran%' OR col ILIKE '%israel%' "
    "OR col ILIKE '%gaza%' OR col ILIKE '%north korea%' OR col ILIKE '%taiwan%' "
    # Healthcare
    "OR col ILIKE '%healthcare%' OR col ILIKE '%medicare%' OR col ILIKE '%medicaid%' "
    "OR col ILIKE '%obamacare%' OR col ILIKE '%abortion%' OR col ILIKE '%reproductive%' "
    "OR col ILIKE '%mental health%' "
    # Civil rights / social issues
    "OR col ILIKE '%gun control%' OR col ILIKE '%gun violence%' OR col ILIKE '%school shooting%' "
    "OR col ILIKE '%supreme court%' OR col ILIKE '%social security%' "
    "OR col ILIKE '%lgbtq%' OR col ILIKE '%trans%' OR col ILIKE '%civil rights%' "
    "OR col ILIKE '%racial justice%' OR col ILIKE '%police reform%' OR col ILIKE '%police brutality%' "
    # Environment / climate
    "OR col ILIKE '%climate change%' OR col ILIKE '%climate policy%' OR col ILIKE '%clean energy%' "
    # Political figures (current admin + prominent opposition)
    "OR col ILIKE '%biden%' OR col ILIKE '%kamala%' OR col ILIKE '%rubio%' "
    "OR col ILIKE '%noem%' OR col ILIKE '%hegseth%' OR col ILIKE '%gabbard%' "
    "OR col ILIKE '%aoc%' OR col ILIKE '%bernie%' OR col ILIKE '%ocasio%') "
)


def _sql_include(col: str) -> str:
    """U.S. politics include conditions as a parenthesised OR block (no leading AND)."""
    return _SQL_INCLUDE_BLOCK.replace("col", col)

def _sql_exclude(col: str) -> str:
    """Exclude conditions as AND NOT ILIKE clauses (leading AND)."""
    return _SQL_EXCLUDE_BLOCK.replace("col", col)

def _sql_kw(col: str) -> str:
    """Full U.S. politics WHERE fragment: include block followed by exclude clauses."""
    return _sql_include(col) + _sql_exclude(col)


# Maps VerbAI category name variants → canonical CATEGORY_META key.
# VerbAI often returns shorter/different names than what the dashboard uses.
CATEGORY_ALIASES: dict[str, str] = {
    # Presidential Politics
    "presidential":                   "Presidential Politics",
    "president":                      "Presidential Politics",
    "white house":                    "Presidential Politics",
    "executive":                      "Presidential Politics",
    "trump":                          "Presidential Politics",
    # General Politics
    "politics":                       "General Politics",
    "political":                      "General Politics",
    "government":                     "General Politics",
    "government & accountability":    "General Politics",
    "government and accountability":  "General Politics",
    # Elections & Voting
    "elections":                      "Elections & Voting",
    "election":                       "Elections & Voting",
    "voting":                         "Elections & Voting",
    "elections & political figures":  "Elections & Voting",
    "elections and political figures":"Elections & Voting",
    # Foreign Policy
    "foreign":                        "Foreign Policy",
    "foreign policy & world":         "Foreign Policy",
    "foreign policy and world":       "Foreign Policy",
    "international":                  "Foreign Policy",
    "geopolitics":                    "Foreign Policy",
    # Immigration Policy
    "immigration":                    "Immigration Policy",
    "border":                         "Immigration Policy",
    "immigration & civil liberties":  "Immigration Policy",
    "immigration and civil liberties":"Immigration Policy",
    # Legislative Politics
    "legislative":                    "Legislative Politics",
    "congress":                       "Legislative Politics",
    "senate":                         "Legislative Politics",
    "legislation":                    "Legislative Politics",
    # Economic Policy
    "economy":                        "Economic Policy",
    "economic":                       "Economic Policy",
    "economics":                      "Economic Policy",
    "finance":                        "Economic Policy",
    "fiscal":                         "Economic Policy",
    "economic inequality":            "Economic Policy",
    "corporate power & consumers":    "Economic Policy",
    "corporate power and consumers":  "Economic Policy",
    "trade":                          "Economic Policy",
    # Healthcare Policy
    "healthcare":                     "Healthcare Policy",
    "health":                         "Healthcare Policy",
    "medical":                        "Healthcare Policy",
    "health policy":                  "Healthcare Policy",
    # Education Policy
    "education":                      "Education Policy",
    "schools":                        "Education Policy",
    "student":                        "Education Policy",
    # Environmental Policy
    "environment":                    "Environmental Policy",
    "environmental":                  "Environmental Policy",
    "climate":                        "Environmental Policy",
    "energy":                         "Environmental Policy",
    "environment & science":          "Environmental Policy",
    "environment and science":        "Environmental Policy",
    # Civil Rights
    "civil rights":                   "Civil Rights",
    "civil liberties":                "Civil Rights",
    "social justice":                 "Civil Rights",
    "criminal justice":               "Civil Rights",
    "culture & media":                "Civil Rights",
    "culture and media":              "Civil Rights",
    "other":                          "General Politics",
}


def _normalize_category(raw: str) -> str:
    """Map a raw VerbAI category string to a canonical CATEGORY_META key."""
    if raw in CATEGORY_META:
        return raw
    lower = raw.lower().strip()
    if lower in CATEGORY_ALIASES:
        return CATEGORY_ALIASES[lower]
    # Partial-match fallback: return the first alias whose key is a substring
    for alias, canonical in CATEGORY_ALIASES.items():
        if alias in lower or lower in alias:
            return canonical
    return "General Politics"


def et_midnight_utc() -> str:
    """Return the ISO Eastern Time timestamp for midnight ET today (NTZ, no Z suffix).

    EVENT_TIME in Snowflake is TIMESTAMP_NTZ stored in Eastern Time.  Passing a
    UTC-expressed literal (e.g. '…05:00:00Z') causes Snowflake to strip the 'Z'
    and treat the value as ET, shifting the cutoff to 5 AM ET instead of midnight.
    We therefore return a plain ET NTZ string so WHERE clauses compare correctly.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    # EDT Apr–Oct (UTC-4), EST Nov–Mar (UTC-5)
    et_hours = 4 if 4 <= now_utc.month <= 10 else 5
    et_tz = datetime.timezone(datetime.timedelta(hours=-et_hours))
    midnight_et = datetime.datetime.combine(
        now_utc.astimezone(et_tz).date(),
        datetime.time.min,
    )
    return midnight_et.strftime("%Y-%m-%dT%H:%M:%S")


def _current_window_start() -> str:
    """Return the start of the current 24-hour sliding window (NTZ, ET-anchored).

    Result = max(WINDOW_ANCHOR, now_ET − 24 h).

    During the first 24 hours after WINDOW_ANCHOR, the anchor itself is returned
    so the window covers everything from Feb 24 midnight ET onward.  After that
    the window slides forward so the dashboard always shows the most recent 24 h.

    Uses the same NTZ / Eastern Time convention as et_midnight_utc() so it can
    be used directly in Snowflake WHERE clauses.
    """
    now_utc  = datetime.datetime.now(datetime.timezone.utc)
    et_hours = 4 if 4 <= now_utc.month <= 10 else 5
    now_et   = (now_utc - datetime.timedelta(hours=et_hours)).replace(tzinfo=None)
    ago_24h  = (now_et - datetime.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    return max(WINDOW_ANCHOR, ago_24h)


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


# Canonical category enumeration used in prompts — single source of truth.
_CAT_ENUM = (
    "Presidential Politics, General Politics, Elections & Voting, Foreign Policy, "
    "Immigration Policy, Legislative Politics, Economic Policy, Healthcare Policy, "
    "Education Policy, Environmental Policy, Civil Rights"
)


def _call_verbai_with_fallbacks(
    prompts: list[str],
    tool_name: str,
    session_id: str | None,
    url: str,
    token: str,
    label: str = "query",
    min_rows: int = 3,
) -> list[dict]:
    """
    Thinking loop for direct MCP VerbAI calls.

    Tries each prompt in turn; stops as soon as >= min_rows rows are returned.
    Always keeps the best (largest) result set seen across all attempts so that
    partial data from attempt 1 is not discarded if attempt 2 is also sparse.

    Using focused, single-topic prompts (one per call) lets Cortex Analyst
    generate clean targeted SQL instead of trying to handle a prompt that
    embeds four raw SQL queries.
    """
    best: list[dict] = []
    for i, prompt in enumerate(prompts):
        text = _call_verbai_agent(prompt, tool_name, session_id, url, token)
        rows = _parse_json_array(text) if text else []
        print(f"[REFINE:{label}] attempt {i + 1}/{len(prompts)} → {len(rows)} rows")
        if len(rows) > len(best):
            best = rows
        if len(best) >= min_rows:
            print(f"[REFINE:{label}] sufficient data — stopping loop")
            break
        if i < len(prompts) - 1:
            print(f"[REFINE:{label}] sparse — trying next prompt")
    return best


# ── Claude subprocess fallback ────────────────────────────────────────────────

def run_verb_ai_query(prompt: str) -> str | None:
    """
    Call the VerbAI MCP tool via the Claude CLI and return the text result.

    Uses claude-haiku for faster token generation and a 120s timeout to
    accommodate CLI startup + MCP initialisation + Snowflake cold-start.
    --max-turns 5 caps the internal agent loop so the process cannot spin
    indefinitely if Cortex Analyst returns sparse results.
    """
    cmd = [
        "claude",
        "--print",
        "--dangerously-skip-permissions",
        "--model", "claude-haiku-4-5-20251001",
        "--max-turns", "5",
        prompt,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
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
        "channel",  # YouTube
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
        f"Use the Snowflake database tool to count political engagement by 18-29 year-olds since {since_iso}. "
        f"Run two SQL queries and combine results by category:\n\n"
        f"QUERY 1 — political search counts by category:\n"
        f"SELECT s.QUERY, COUNT(*) AS cnt, COUNT(DISTINCT s.USER_ID) AS users "
        f"FROM SEARCH_EVENTS_FLAT_DYM s "
        f"JOIN AGENT_SYNC a ON s.USER_ID = a.USER_ID "
        f"WHERE s.EVENT_TIME >= '{since_iso}' "
        f"AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29 "
        f"AND {_sql_kw('s.QUERY')}"
        f"GROUP BY s.QUERY;\n\n"
        f"QUERY 2 — political Reddit post counts by category:\n"
        f"SELECT r.TITLE AS query, COUNT(*) AS cnt, COUNT(DISTINCT r.USER_ID) AS users, r.SUBREDDIT "
        f"FROM REDDIT_EVENTS_FLAT_DYM r "
        f"JOIN AGENT_SYNC a ON r.USER_ID = a.USER_ID "
        f"WHERE r.EVENT_TIME >= '{since_iso}' "
        f"AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29 "
        f"AND (LOWER(r.SUBREDDIT) IN ('politics','politicaldiscussion','conservative','liberal',"
        f"'neutralpolitics','geopolitics','economics','economy','environment',"
        f"'climate','healthcare','immigration','supremecourt','law','progressive','democrats',"
        f"'republican','political_humor','libertarian','uspolitics','americanpolitics') "
        f"OR {_sql_include('r.TITLE')}) "
        f"{_sql_exclude('r.TITLE')}"
        f"GROUP BY r.TITLE, r.SUBREDDIT;\n\n"
        f"For each search query or Reddit post, assign it to the ONE most relevant category from: "
        f"Presidential Politics, General Politics, Elections & Voting, Foreign Policy, "
        f"Immigration Policy, Legislative Politics, Economic Policy, Healthcare Policy, "
        f"Education Policy, Environmental Policy, Civil Rights. "
        f"Then sum engagement_count and unique_users per category. "
        f"Respond ONLY with a raw JSON array (no markdown, no explanation) with objects: "
        f"policy_category, engagement_count, unique_users."
    )

    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx
        text = _call_verbai_agent(prompt, tool_name, session_id, url, token)
        if text:
            result = _parse_json_array(text)
            print(f"[MCP-DIRECT] category_counts: {len(result)} rows")
            return result
        if mcp_ctx:
            print("[MCP-DIRECT] category_counts: sparse (MCP active) — skipping Claude fallback")
            return []
        print("[MCP-DIRECT] category_counts returned empty — falling back to Claude")

    text = run_verb_ai_query(prompt)
    return _parse_json_array(text) if text else []


# ── TikTok oEmbed title resolver ──────────────────────────────────────────────
# In-process URL → title cache (avoids re-fetching the same URL within one run
# and persists if the module stays loaded in a long-running process).
_TIKTOK_TITLE_CACHE: dict[str, str | None] = {}

_TIKTOK_OEMBED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def resolve_tiktok_oembed(video_url: str) -> tuple[str | None, str | None]:
    """
    Call TikTok's public oEmbed endpoint to resolve a video URL to its title
    and author.  Works with both the share URL format used in DONATIONS_EVENTS_DYM:
        https://www.tiktokv.com/share/video/{id}/
    and the canonical format:
        https://www.tiktok.com/@user/video/{id}

    Returns (title, author_name) or (None, None) on any error.
    Results are cached in _TIKTOK_TITLE_CACHE.
    """
    if video_url in _TIKTOK_TITLE_CACHE:
        cached = _TIKTOK_TITLE_CACHE[video_url]
        if cached is None:
            return None, None
        return cached, None  # author not stored in cache — caller only needs title

    oembed_url = (
        "https://www.tiktok.com/oembed?url="
        + urllib.parse.quote(video_url, safe="")
    )
    try:
        req = urllib.request.Request(oembed_url, headers=_TIKTOK_OEMBED_HEADERS)
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        title  = (data.get("title")       or "").strip() or None
        author = (data.get("author_name") or "").strip() or None
        _TIKTOK_TITLE_CACHE[video_url] = title
        return title, author
    except Exception as exc:
        print(f"[WARN] TikTok oEmbed failed for {video_url[:70]}: {exc}")
        _TIKTOK_TITLE_CACHE[video_url] = None
        return None, None


def _dedup_query_items(items: list[dict]) -> list[dict]:
    """
    Deduplicate aggregated query/Reddit items by lowercased query text.
    When the same query appears in both the popularity-ordered and
    recency-ordered passes, keep the entry with the higher count.
    """
    seen: dict[str, int] = {}   # lower query → index in result
    result: list[dict] = []
    for item in items:
        key = (item.get("query") or "").lower().strip()[:80]
        if key in seen:
            # Accumulate counts so the merged item reflects total engagement
            result[seen[key]]["count"] = max(
                result[seen[key]].get("count", 0), item.get("count", 0)
            )
        else:
            seen[key] = len(result)
            result.append(item)
    return result


def fetch_search_queries(since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Query political search queries and Reddit posts for 18-29 year-olds.

    Direct MCP path (thinking loop): two separate VerbAI calls — one for search
    events, one for Reddit posts — each with a focused natural-language primary
    prompt and a broader fallback.  Cortex Analyst generates cleaner SQL from
    single-topic prompts than from a prompt containing multiple embedded SQL blocks.

    Claude subprocess path: an exploratory prompt that explicitly permits Claude
    to call VerbAI multiple times, inspect results, and refine before answering.

    Returns list of dicts with keys: query, topic, count, source, subreddit, category, trend.
    """
    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx

        # ── Search events: focused prompts, progressively broader ─────────────
        search_prompts = [
            # Attempt 1: targeted with age filter, both popularity and recency
            (
                f"What are the top political search queries by users aged 18 to 29 "
                f"since {since_iso}? Include both most-searched and most-recently searched. "
                f"Topics: Trump, DOGE, immigration, tariffs, Congress, federal workers, "
                f"healthcare, Ukraine, climate change, student debt, social security. "
                f"Return JSON: [{{\"query\": str, \"count\": int, \"category\": str, "
                f"\"source\": \"search\", \"trend\": \"up\"|\"down\"|\"stable\"}}]. "
                f"Categories: {_CAT_ENUM}. Use 'General Politics' only if no other fits."
            ),
            # Attempt 2: drop explicit age, simpler phrasing — may generate better SQL
            (
                f"Show me the top US political search queries since {since_iso}. "
                f"Include any searches about Trump, Congress, immigration, tariffs, "
                f"DOGE, or government policy. "
                f"Return JSON: [{{\"query\": str, \"count\": int}}]"
            ),
        ]

        # ── Reddit posts: focused prompts, progressively broader ──────────────
        reddit_prompts = [
            # Attempt 1: targeted with age filter and explicit subreddit list
            (
                f"What are the top political Reddit posts by users aged 18 to 29 "
                f"since {since_iso}? Include both most-engaged and most-recent posts. "
                f"Focus on r/politics, r/politicaldiscussion, r/conservative, r/liberal, "
                f"r/neutralpolitics, r/economics, r/immigration, r/supremecourt, "
                f"r/progressive, r/democrats, r/republican. "
                f"Return JSON: [{{\"query\": str, \"count\": int, \"subreddit\": str, "
                f"\"category\": str, \"source\": \"reddit\", \"trend\": \"up\"|\"down\"|\"stable\"}}]. "
                f"Categories: {_CAT_ENUM}. Use 'General Politics' only if no other fits."
            ),
            # Attempt 2: broader — any US politics discussion on Reddit
            (
                f"Top US political Reddit discussions since {since_iso}. "
                f"Any subreddit covering politics, government, elections, or policy. "
                f"Return JSON: [{{\"query\": str, \"subreddit\": str, \"count\": int}}]"
            ),
        ]

        # ── TikTok search history (DONATIONS_EVENTS_DYM) ─────────────────────
        # NOTE: The VerbAI/Cortex Analyst semantic model does not currently expose
        # the search_term JSON field inside DONATIONS_EVENTS_DYM.DATA for the
        # search_history event type.  These prompts will return 0 rows until VerbAI
        # updates the semantic model to include that dimension.  The infrastructure
        # is wired up and ready — no code change needed once the model is updated.
        tiktok_search_prompts = [
            (
                f"What are the top political TikTok search terms by users aged 18 to 29 "
                f"since {since_iso}? Query the DONATIONS_EVENTS_DYM table where "
                f"EVENT_TYPE = 'search_history' and PLATFORM = 'tiktok'. The DATA column "
                f"is JSON containing a search_term field. Filter for political terms: "
                f"Trump, DOGE, immigration, tariffs, Congress, federal, healthcare, Ukraine, "
                f"climate change, abortion, gun control, student debt, social security. "
                f"Return JSON: [{{\"query\": str, \"count\": int, \"category\": str, "
                f"\"source\": \"tiktok_search\", \"trend\": \"up\"|\"down\"|\"stable\"}}]. "
                f"Categories: {_CAT_ENUM}."
            ),
            (
                f"Top political searches on TikTok since {since_iso}. "
                f"Use DONATIONS_EVENTS_DYM where EVENT_TYPE='search_history' and PLATFORM='tiktok'. "
                f"Filter DATA field for political keywords (Trump, immigration, tariffs, DOGE, Congress). "
                f"Return JSON: [{{\"query\": str, \"count\": int}}]"
            ),
        ]

        search_rows = _call_verbai_with_fallbacks(
            search_prompts, tool_name, session_id, url, token, label="search"
        )
        reddit_rows = _call_verbai_with_fallbacks(
            reddit_prompts, tool_name, session_id, url, token, label="reddit"
        )
        tiktok_search_rows = _call_verbai_with_fallbacks(
            tiktok_search_prompts, tool_name, session_id, url, token, label="tiktok-search"
        )

        # Ensure source field is set (fallback prompts may omit it)
        for r in search_rows:
            r.setdefault("source", "search")
        for r in reddit_rows:
            r.setdefault("source", "reddit")
        for r in tiktok_search_rows:
            r.setdefault("source", "tiktok_search")

        combined = search_rows + reddit_rows + tiktok_search_rows
        if combined:
            raw = _dedup_query_items(combined)
            result = [item for item in raw if _is_political_item(item)]
            print(
                f"[MCP-DIRECT] search_queries: search={len(search_rows)} "
                f"reddit={len(reddit_rows)} tiktok={len(tiktok_search_rows)} "
                f"→ {len(result)} political"
            )
            return result
        # MCP session was active but returned no data — Claude queries the same
        # VerbAI source and will also return nothing.  Skip the subprocess.
        if mcp_ctx:
            print("[MCP-DIRECT] search_queries: sparse (MCP active) — skipping Claude fallback")
            return []
        print("[MCP-DIRECT] search_queries: all attempts sparse — falling back to Claude")

    # ── Claude subprocess: exploratory prompt that permits iteration ──────────
    # Claude's internal agent loop can call VerbAI multiple times.  The prompt
    # tells it to probe first and refine rather than firing one rigid query.
    claude_prompt = (
        f"You have the VerbAI tool.  Find political search, Reddit, and TikTok search data for "
        f"18-29 year olds since {since_iso}.\n\n"
        f"Work iteratively:\n"
        f"1. Query SEARCH events for political topics — Trump, immigration, tariffs, "
        f"DOGE, federal workers, Congress, healthcare, Ukraine, climate.\n"
        f"2. Query REDDIT posts from political subreddits — r/politics, r/conservative, "
        f"r/liberal, r/politicaldiscussion, r/economics, r/immigration, r/supremecourt.\n"
        f"3. Query DONATIONS_EVENTS_DYM where EVENT_TYPE='search_history' and PLATFORM='tiktok' "
        f"for political search terms (Trump, immigration, tariffs, DOGE, Congress, healthcare). "
        f"The DATA column is JSON with a search_term field.\n"
        f"4. If a query returns no data, try a broader phrasing or slightly wider time window.\n"
        f"5. Combine all results.\n\n"
        f"Once done, output ONLY a raw JSON array (no markdown):\n"
        f"[{{\"query\": str, \"count\": int, \"source\": \"search\"|\"reddit\"|\"tiktok_search\", "
        f"\"subreddit\": str|null, \"category\": str, \"trend\": \"up\"|\"down\"|\"stable\"}}]\n"
        f"Categories: {_CAT_ENUM}."
    )
    text = run_verb_ai_query(claude_prompt)
    raw = _dedup_query_items(_parse_json_array(text) if text else [])
    return [item for item in raw if _is_political_item(item)]


# Lowercase → canonical label lookup built once at import time
_CANONICAL_CATEGORY: dict[str, str] = {k.lower(): k for k in CATEGORY_META}

# Keywords that alone are sufficient to identify US political content.
_STRONG_POLITICAL_KEYWORDS: frozenset[str] = frozenset({
    # Trump administration
    "trump", "white house", "maga", "executive order",
    # Congress / legislation
    "congress", "senate", "house of representatives", "filibuster",
    "legislation", "democrat", "republican",
    # DOGE / federal spending
    "doge", "elon musk", "federal worker", "federal employee",
    "federal budget", "government efficiency", "spending cut",
    # Economy (US-specific)
    "tariff", "tariffs", "trade war", "inflation", "unemployment",
    # Immigration
    "immigration", "deportation", "ice raid", "daca",
    "undocumented", "migrant",
    # Domestic policy
    "healthcare", "medicare", "medicaid", "obamacare", "aca",
    "abortion", "gun control", "gun violence", "second amendment",
    "supreme court", "social security", "student debt",
    "climate change", "climate policy",
    # Budget / debt
    "deficit", "debt ceiling", "appropriations",
    # SOTU-specific
    "state of the union", "sotu", "address to congress",
    # US political figures
    "biden", "kamala", "harris", "rubio", "noem", "hegseth",
    "gabbard", "patel",
    # Sanctions (usually in US foreign policy context)
    "sanction", "sanctions",
})

# Country / region names that are only political if paired with a second signal.
# Bare mentions ("live in china", "lionel richie china gala") are filtered out.
_COUNTRY_KEYWORDS: frozenset[str] = frozenset({
    "ukraine", "russia", "nato", "china", "taiwan", "iran", "israel",
    "gaza", "middle east", "north korea",
})

# Second-tier signals: combined with a country keyword they confirm political context.
_COUNTRY_CONTEXT_KEYWORDS: frozenset[str] = frozenset({
    "war", "missile", "military", "soldier", "bomb", "attack",
    "policy", "aid", "deal", "treaty", "alliance", "conflict", "nuclear",
    "diplomat", "minister", "president", "election", "coup", "protest",
    "weapon", "troops", "invasion", "occupied", "ceasefire",
    "parliament", "foreign", "bilateral", "tariff", "tariffs",
    "trade", "sanction", "sanctions",
})

# Phrases that disqualify an item even if it passes a keyword check —
# catches foreign-political content that leaks through broad terms like
# "arrest", "president", "coup", "parliament", etc.
_SOTU_BLOCKLIST: tuple[str, ...] = (
    "prince andrew", "prince william", "prince harry", "royal family",
    "buckingham", "king charles",
    "south korea", "yoon suk", "korean president",
    "epstein",  # tabloid context; not US policy
    "blondie in china", "white girl in china",  # viral non-political
)


def _kw_in(keyword: str, clean_padded: str) -> bool:
    """True if keyword appears as a whole word/phrase in clean_padded text.

    clean_padded must be the output of `' ' + _CLEAN_RE.sub(' ', text.lower()) + ' '`
    (non-alpha chars replaced with spaces, padded with leading/trailing spaces).
    Matching `' keyword '` ensures 'maga' doesn't fire on 'magan', etc.
    Multi-word keywords work naturally: ' trade war ' matches in ' china trade war '.
    """
    return f" {keyword} " in clean_padded


def _is_political_item(item: dict) -> bool:
    """
    Return True if this item is SOTU-relevant US politics.

    Two-tier matching (word-boundary aware — 'maga' will NOT match 'magan'):
    - Tier 1 (strong): any whole-word match from _STRONG_POLITICAL_KEYWORDS → pass.
    - Tier 2 (country): a country/region name alone is NOT enough.  It must be
      accompanied by at least one word from _COUNTRY_CONTEXT_KEYWORDS to confirm
      the item is about US foreign / geopolitical policy, not travel or culture
      content that happens to mention "china" or "iran".

    Blocklist is applied first regardless.
    """
    raw  = (item.get("query") or item.get("topic") or "").lower()
    # Replace all non-alpha chars with spaces; pad for whole-word boundary checks
    text = " " + _CLEAN_RE.sub(" ", raw) + " "

    # Blocklist: drop known irrelevant content regardless of other signals
    if any(phrase in raw for phrase in _SOTU_BLOCKLIST):
        return False

    # Tier 1: unambiguous US political keyword → immediate pass
    if any(_kw_in(kw, text) for kw in _STRONG_POLITICAL_KEYWORDS):
        return True

    # Tier 2: country/region name needs a second political-context word
    if any(_kw_in(kw, text) for kw in _COUNTRY_KEYWORDS):
        return any(_kw_in(kw, text) for kw in _COUNTRY_CONTEXT_KEYWORDS)

    return False


def _filter_to_political(events: list[dict]) -> list[dict]:
    """
    Drop events whose category is not one of the 11 canonical political labels.
    Also normalises the category field to the exact canonical string so the
    frontend's category matching never silently fails (e.g. 'foreign policy'
    becomes 'Foreign Policy').
    """
    result = []
    for ev in events:
        raw = (ev.get("category") or "").strip()
        canonical = _CANONICAL_CATEGORY.get(raw.lower())
        if canonical:
            ev = dict(ev)          # don't mutate the original
            ev["category"] = canonical
            result.append(ev)
        # else: silently drop 'sports', 'entertainment', etc.
    return result


def _cap_events_per_user(events: list[dict], max_per_user: int = 3) -> list[dict]:
    """Keep at most max_per_user events per unique (age, gender, state) fingerprint."""
    counts: dict[str, int] = {}
    result = []
    for ev in events:
        key = f"{ev.get('age','?')}|{ev.get('gender','?')}|{ev.get('state','?')}"
        if counts.get(key, 0) < max_per_user:
            result.append(ev)
            counts[key] = counts.get(key, 0) + 1
    return result


def _dedup_live_events(events: list[dict]) -> list[dict]:
    """
    Remove exact duplicate live events by (time, lowercased query) key.
    Preserves order (caller should sort afterwards).
    """
    seen: set[tuple] = set()
    result: list[dict] = []
    for ev in events:
        key = (ev.get("time", ""), (ev.get("query") or "").lower().strip()[:60])
        if key not in seen:
            seen.add(key)
            result.append(ev)
    return result


def fetch_live_events(live_since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Query individual political events for 18-29 year-olds across today's full window.
    Splits the day at its midpoint and queries each half separately so early-morning
    activity is not buried by the recency sort.  Four queries total (search + Reddit
    × early half + recent half), merged and deduplicated in Python.
    Returns list of dicts with keys: time, query, source, subreddit, category, age, gender, state.
    Per-user capping (max 3 events per person) is applied after retrieval.
    """
    # Mid-point of today's window: used to split queries across the full day.
    # live_since_iso is ET NTZ (no Z suffix); compute now in ET for consistent arithmetic.
    since_dt  = datetime.datetime.strptime(live_since_iso, "%Y-%m-%dT%H:%M:%S")
    _now_utc  = datetime.datetime.now(datetime.timezone.utc)
    _et_hours = 4 if 4 <= _now_utc.month <= 10 else 5
    now_dt    = (_now_utc - datetime.timedelta(hours=_et_hours)).replace(tzinfo=None)
    mid_dt    = since_dt + (now_dt - since_dt) / 2
    mid_iso  = mid_dt.strftime("%Y-%m-%dT%H:%M:%S")

    reddit_subreddits = (
        "LOWER(r.SUBREDDIT) IN ('politics','politicaldiscussion','conservative','liberal',"
        "'neutralpolitics','geopolitics','economics','economy','environment',"
        "'climate','healthcare','immigration','supremecourt','law','progressive','democrats',"
        "'republican','political_humor','libertarian','uspolitics','americanpolitics')"
    )

    prompt = (
        f"Use the Snowflake database tool to get a comprehensive sample of political events "
        f"by 18-29 year-olds across today's full window (since {live_since_iso}). "
        f"Split the day at its midpoint ({mid_iso}) and run FOUR queries so early-day "
        f"and recent activity are both represented:\n\n"
        f"QUERY 1 — recent political searches ({mid_iso} → now):\n"
        f"SELECT s.EVENT_TIME AS time, s.QUERY AS query, 'search' AS source, "
        f"NULL AS subreddit, a.YEAR_OF_BIRTH, a.GENDER, a.FULL_ADDRESS "
        f"FROM SEARCH_EVENTS_FLAT_DYM s "
        f"JOIN AGENT_SYNC a ON s.USER_ID = a.USER_ID "
        f"WHERE s.EVENT_TIME >= '{mid_iso}' "
        f"AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29 "
        f"AND {_sql_kw('s.QUERY')}"
        f"ORDER BY s.EVENT_TIME DESC LIMIT 120;\n\n"
        f"QUERY 2 — early-day political searches ({live_since_iso} → {mid_iso}):\n"
        f"SELECT s.EVENT_TIME AS time, s.QUERY AS query, 'search' AS source, "
        f"NULL AS subreddit, a.YEAR_OF_BIRTH, a.GENDER, a.FULL_ADDRESS "
        f"FROM SEARCH_EVENTS_FLAT_DYM s "
        f"JOIN AGENT_SYNC a ON s.USER_ID = a.USER_ID "
        f"WHERE s.EVENT_TIME >= '{live_since_iso}' AND s.EVENT_TIME < '{mid_iso}' "
        f"AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29 "
        f"AND {_sql_kw('s.QUERY')}"
        f"ORDER BY s.EVENT_TIME DESC LIMIT 80;\n\n"
        f"QUERY 3 — recent political Reddit posts ({mid_iso} → now):\n"
        f"SELECT r.EVENT_TIME AS time, r.TITLE AS query, 'reddit' AS source, "
        f"r.SUBREDDIT AS subreddit, a.YEAR_OF_BIRTH, a.GENDER, a.FULL_ADDRESS "
        f"FROM REDDIT_EVENTS_FLAT_DYM r "
        f"JOIN AGENT_SYNC a ON r.USER_ID = a.USER_ID "
        f"WHERE r.EVENT_TIME >= '{mid_iso}' "
        f"AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29 "
        f"AND ({reddit_subreddits} OR {_sql_include('r.TITLE')}) "
        f"{_sql_exclude('r.TITLE')}"
        f"ORDER BY r.EVENT_TIME DESC LIMIT 80;\n\n"
        f"QUERY 4 — early-day political Reddit posts ({live_since_iso} → {mid_iso}):\n"
        f"SELECT r.EVENT_TIME AS time, r.TITLE AS query, 'reddit' AS source, "
        f"r.SUBREDDIT AS subreddit, a.YEAR_OF_BIRTH, a.GENDER, a.FULL_ADDRESS "
        f"FROM REDDIT_EVENTS_FLAT_DYM r "
        f"JOIN AGENT_SYNC a ON r.USER_ID = a.USER_ID "
        f"WHERE r.EVENT_TIME >= '{live_since_iso}' AND r.EVENT_TIME < '{mid_iso}' "
        f"AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29 "
        f"AND ({reddit_subreddits} OR {_sql_include('r.TITLE')}) "
        f"{_sql_exclude('r.TITLE')}"
        f"ORDER BY r.EVENT_TIME DESC LIMIT 50;\n\n"
        f"For EVENT_TIME format as ISO 8601 (e.g. 2026-02-20T14:32:00Z). "
        f"For age compute YEAR(CURRENT_DATE) - YEAR_OF_BIRTH as an integer. "
        f"For state extract the 2-letter US state abbreviation from FULL_ADDRESS. "
        f"For each event assign the ONE most relevant category from this EXACT list (verbatim): "
        f"Presidential Politics, General Politics, Elections & Voting, Foreign Policy, "
        f"Immigration Policy, Legislative Politics, Economic Policy, Healthcare Policy, "
        f"Education Policy, Environmental Policy, Civil Rights. "
        f"Respond ONLY with a raw JSON array (no markdown, no explanation) with objects: "
        f"time, query, source, subreddit, category, age, gender, state. "
        f"Merge all four query results, sort by time descending, return up to 330 events."
    )

    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx
        text = _call_verbai_agent(prompt, tool_name, session_id, url, token)
        if text:
            result = _parse_json_array(text)
            political = _filter_to_political(result)
            print(f"[MCP-DIRECT] live_events: {len(result)} rows → {len(political)} political")
            return _cap_events_per_user(political)
        if mcp_ctx:
            print("[MCP-DIRECT] live_events: sparse (MCP active) — skipping Claude fallback")
            return []
        print("[MCP-DIRECT] live_events returned empty — falling back to Claude")

    text = run_verb_ai_query(prompt)
    raw = _parse_json_array(text) if text else []
    political = _filter_to_political(raw)
    print(f"[INFO] live_events: {len(raw)} rows → {len(political)} political after filter")
    return _cap_events_per_user(political)


def fetch_youtube_videos(since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Query political YouTube videos watched by 18-29 year-olds since since_iso.

    Direct MCP path (thinking loop): two VerbAI calls with focused prompts —
    most-watched videos first, then most-recently-watched.  Each has a broader
    fallback prompt tried automatically when results are sparse.

    Claude subprocess path: exploratory prompt that permits iterative querying.

    Returns list of dicts with keys: query (video title), topic, count, source
    ('youtube'), channel, category, trend.
    """
    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx

        # ── Most-watched political YouTube videos ─────────────────────────────
        popular_prompts = [
            # Attempt 1: targeted, age-filtered, by view count
            (
                f"What are the most-watched political YouTube videos by users aged 18 to 29 "
                f"since {since_iso}? Topics: Trump, immigration, tariffs, DOGE, Congress, "
                f"federal government, healthcare, Ukraine, climate change. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int, "
                f"\"category\": str, \"source\": \"youtube\", \"trend\": \"up\"|\"down\"|\"stable\"}}]. "
                f"Categories: {_CAT_ENUM}. Use 'General Politics' only if no other fits."
            ),
            # Attempt 2: drop explicit age, simpler
            (
                f"Top political YouTube videos watched today since {since_iso}. "
                f"US politics, government, elections, policy topics. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int}}]"
            ),
        ]

        # ── Most-recently-watched political YouTube videos ────────────────────
        recent_prompts = [
            # Attempt 1: targeted, age-filtered, by recency
            (
                f"What political YouTube videos have users aged 18 to 29 watched most "
                f"recently since {since_iso}? Emphasise newly uploaded content. "
                f"Topics: Trump, DOGE, immigration, tariffs, Congress, federal workers, "
                f"healthcare, Ukraine, climate. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int, "
                f"\"category\": str, \"source\": \"youtube\", \"trend\": \"up\"|\"down\"|\"stable\"}}]. "
                f"Categories: {_CAT_ENUM}. Use 'General Politics' only if no other fits."
            ),
            # Attempt 2: broader — any recently trending political YouTube content
            (
                f"Most recently trending political YouTube videos since {since_iso}. "
                f"US government, Trump, Congress, immigration, elections. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int}}]"
            ),
        ]

        popular_rows = _call_verbai_with_fallbacks(
            popular_prompts, tool_name, session_id, url, token, label="yt-popular"
        )
        recent_rows = _call_verbai_with_fallbacks(
            recent_prompts, tool_name, session_id, url, token, label="yt-recent"
        )

        for r in popular_rows + recent_rows:
            r.setdefault("source", "youtube")

        combined = popular_rows + recent_rows
        if combined:
            raw = _dedup_query_items(combined)
            result = [item for item in raw if _is_political_item(item)]
            print(
                f"[MCP-DIRECT] youtube_videos: popular={len(popular_rows)} "
                f"recent={len(recent_rows)} → {len(result)} political"
            )
            return result
        # MCP session was active but returned no data — Claude queries the same
        # VerbAI source and will also return nothing.  Skip the subprocess.
        if mcp_ctx:
            print("[MCP-DIRECT] youtube_videos: sparse (MCP active) — skipping Claude fallback")
            return []
        print("[MCP-DIRECT] youtube_videos: all attempts sparse — falling back to Claude")

    # ── Claude subprocess: exploratory prompt that permits iteration ──────────
    claude_prompt = (
        f"You have the VerbAI tool.  Find political YouTube videos watched by "
        f"18-29 year olds since {since_iso}.\n\n"
        f"Work iteratively:\n"
        f"1. Query for most-watched political YouTube videos (Trump, immigration, "
        f"tariffs, DOGE, Congress, healthcare, Ukraine, climate).\n"
        f"2. Query for most-recently-watched political YouTube videos.\n"
        f"3. If a query returns no data, try a broader phrasing or wider time window.\n"
        f"4. Combine both result sets.\n\n"
        f"Once done, output ONLY a raw JSON array (no markdown):\n"
        f"[{{\"query\": str, \"channel\": str, \"count\": int, "
        f"\"source\": \"youtube\", \"category\": str, \"trend\": \"up\"|\"down\"|\"stable\"}}]\n"
        f"Categories: {_CAT_ENUM}."
    )
    text = run_verb_ai_query(claude_prompt)
    raw = _dedup_query_items(_parse_json_array(text) if text else [])
    result = [item for item in raw if _is_political_item(item)]
    print(f"[INFO] youtube_videos: {len(raw)} rows → {len(result)} political")
    return result


def fetch_tiktok_watch_videos(since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Fetch political TikTok videos watched by 18-29 year-olds since since_iso.

    Two-step process:
      1. VerbAI — query DONATIONS_EVENTS_DYM for watch_history events on TikTok,
         grouped by video URL with watch counts.  Returns the top-watched URLs.
      2. oEmbed  — resolve each URL to a video title via TikTok's public oEmbed API
         (https://www.tiktok.com/oembed), then filter for political content.

    Both the tiktokv.com/share/video/{id} format (used in DONATIONS_EVENTS_DYM)
    and the canonical tiktok.com/@user/video/{id} format are supported.

    Returns list of dicts: query (video title), count, source ("tiktok_watch"),
    channel (author), category, trend.
    """
    # Use explicit Snowflake JSON semi-structured syntax (DATA:link::varchar) so
    # Cortex Analyst generates the correct extraction SQL rather than guessing.
    # This mirrors the approach used by fetch_live_events (raw SQL embedded in prompt).
    url_count_prompts = [
        (
            f"Use the Snowflake database tool. Run this exact SQL:\n\n"
            f"SELECT DATA:link::varchar AS url, COUNT(*) AS cnt\n"
            f"FROM DONATIONS_EVENTS_DYM\n"
            f"WHERE EVENT_TYPE = 'watch_history'\n"
            f"  AND PLATFORM = 'tiktok'\n"
            f"  AND EVENT_TIME >= '{since_iso}'\n"
            f"  AND (DATA:link::varchar ILIKE '%tiktok%'\n"
            f"       OR DATA:link::varchar ILIKE '%tiktokv%')\n"
            f"GROUP BY url\n"
            f"ORDER BY cnt DESC\n"
            f"LIMIT 60;\n\n"
            f"Return JSON only: [{{\"url\": str, \"count\": int}}]"
        ),
        (
            f"Use the Snowflake database tool. Run this SQL:\n\n"
            f"SELECT PARSE_JSON(DATA)['link']::varchar AS url, COUNT(*) AS cnt\n"
            f"FROM DONATIONS_EVENTS_DYM\n"
            f"WHERE EVENT_TYPE = 'watch_history' AND PLATFORM = 'tiktok'\n"
            f"  AND EVENT_TIME >= '{since_iso}'\n"
            f"GROUP BY url HAVING url IS NOT NULL\n"
            f"ORDER BY cnt DESC LIMIT 60;\n\n"
            f"Return JSON: [{{\"url\": str, \"count\": int}}]"
        ),
        (
            f"Top TikTok video URLs watched since {since_iso}. "
            f"DONATIONS_EVENTS_DYM, EVENT_TYPE='watch_history', PLATFORM='tiktok'. "
            f"Extract video link from DATA column JSON field named 'link'. "
            f"Group by URL, count rows, order descending. "
            f"Return JSON: [{{\"url\": str, \"count\": int}}]"
        ),
    ]

    if mcp_ctx:
        tool_name, session_id, mcp_url, token = mcp_ctx
        url_rows = _call_verbai_with_fallbacks(
            url_count_prompts, tool_name, session_id, mcp_url, token,
            label="tiktok-watch-urls",
        )
    else:
        text = run_verb_ai_query(url_count_prompts[0])
        url_rows = _parse_json_array(text) if text else []

    if not url_rows:
        print("[INFO] tiktok_watch: no watch URLs returned from VerbAI")
        return []

    # Sort by count descending; cap at 60 resolutions to stay within oEmbed rate limits.
    url_rows.sort(key=lambda x: x.get("count", 0), reverse=True)
    resolved: list[dict] = []
    for row in url_rows[:60]:
        video_url = (row.get("url") or "").strip()
        if not video_url:
            continue
        title, author = resolve_tiktok_oembed(video_url)
        if title:
            resolved.append({
                "query":   title,
                "count":   int(row.get("count") or 1),
                "source":  "tiktok_watch",
                "channel": author or "",
                "url":     video_url,
            })
        time.sleep(0.15)  # gentle pacing to avoid oEmbed rate limiting

    print(f"[INFO] tiktok_watch: {len(url_rows)} URLs → {len(resolved)} titles resolved")
    political = [item for item in resolved if _is_political_item(item)]
    print(f"[INFO] tiktok_watch: {len(resolved)} resolved → {len(political)} political")
    return political


def fetch_news_articles(since_iso: str, mcp_ctx: tuple | None = None) -> list[dict]:
    """
    Query political news articles read by 18-29 year-olds from NEWS_EVENTS_FLAT_DYM.

    Direct MCP path: two VerbAI calls — most-read articles first, then most-recent.
    Each has a broader fallback prompt.

    Returns list of dicts with keys: query (article title), count, source ('news'),
    channel (news outlet), category, trend.
    """
    if mcp_ctx:
        tool_name, session_id, url, token = mcp_ctx

        popular_prompts = [
            (
                f"What are the most-read political news articles by users aged 18 to 29 "
                f"since {since_iso}? Query NEWS_EVENTS_FLAT_DYM joined with AGENT_SYNC on USER_ID. "
                f"Filter where YEAR(CURRENT_DATE) - YEAR_OF_BIRTH BETWEEN 18 AND 29 and "
                f"title or keywords contain: Trump, DOGE, immigration, tariffs, Congress, "
                f"federal, healthcare, Ukraine, climate, abortion, gun control, student debt, "
                f"social security, supreme court. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int, "
                f"\"category\": str, \"source\": \"news\", \"trend\": \"up\"|\"down\"|\"stable\"}}]. "
                f"Use NEWS_OUTLET as the channel field. "
                f"Categories: {_CAT_ENUM}. Use 'General Politics' only if no other fits."
            ),
            (
                f"Top political news articles read today since {since_iso} from NEWS_EVENTS_FLAT_DYM. "
                f"US politics, Trump, government, elections, policy topics. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int}}]"
            ),
        ]

        recent_prompts = [
            (
                f"What political news articles have users aged 18 to 29 read most recently "
                f"since {since_iso}? Query NEWS_EVENTS_FLAT_DYM joined with AGENT_SYNC. "
                f"Filter for users aged 18-29 and articles with political keywords in title or keywords. "
                f"Order by EVENT_TIME descending. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int, "
                f"\"category\": str, \"source\": \"news\", \"trend\": \"up\"|\"down\"|\"stable\"}}]. "
                f"Use NEWS_OUTLET as the channel field. Categories: {_CAT_ENUM}."
            ),
            (
                f"Most recently read political news from NEWS_EVENTS_FLAT_DYM since {since_iso}. "
                f"US government, Trump, Congress, immigration, elections. "
                f"Return JSON: [{{\"query\": str, \"channel\": str, \"count\": int}}]"
            ),
        ]

        popular_rows = _call_verbai_with_fallbacks(
            popular_prompts, tool_name, session_id, url, token, label="news-popular"
        )
        recent_rows = _call_verbai_with_fallbacks(
            recent_prompts, tool_name, session_id, url, token, label="news-recent"
        )

        for r in popular_rows + recent_rows:
            r.setdefault("source", "news")

        combined = popular_rows + recent_rows
        if combined:
            raw = _dedup_query_items(combined)
            result = [item for item in raw if _is_political_item(item)]
            print(
                f"[MCP-DIRECT] news_articles: popular={len(popular_rows)} "
                f"recent={len(recent_rows)} → {len(result)} political"
            )
            return result
        # MCP session was active but returned no data — Claude queries the same
        # VerbAI source and will also return nothing.  Skip the subprocess.
        if mcp_ctx:
            print("[MCP-DIRECT] news_articles: sparse (MCP active) — skipping Claude fallback")
            return []
        print("[MCP-DIRECT] news_articles: all attempts sparse — falling back to Claude")

    # ── Claude subprocess: exploratory prompt that permits iteration ──────────
    claude_prompt = (
        f"You have the VerbAI tool.  Find political news articles read by "
        f"18-29 year olds since {since_iso}.\n\n"
        f"Work iteratively:\n"
        f"1. Query NEWS_EVENTS_FLAT_DYM joined with AGENT_SYNC on USER_ID for users aged 18-29. "
        f"Filter for articles with political keywords in TITLE or KEYWORDS: "
        f"Trump, immigration, tariffs, DOGE, Congress, federal workers, "
        f"healthcare, Ukraine, climate, abortion, supreme court.\n"
        f"2. Also query for most recently read political articles (order by EVENT_TIME DESC).\n"
        f"3. If a query returns no data, try a broader phrasing or wider time window.\n"
        f"4. Combine both result sets.\n\n"
        f"Once done, output ONLY a raw JSON array (no markdown):\n"
        f"[{{\"query\": str, \"channel\": str, \"count\": int, "
        f"\"source\": \"news\", \"category\": str, \"trend\": \"up\"|\"down\"|\"stable\"}}]\n"
        f"Use NEWS_OUTLET as the channel field. Categories: {_CAT_ENUM}."
    )
    text = run_verb_ai_query(claude_prompt)
    raw = _dedup_query_items(_parse_json_array(text) if text else [])
    result = [item for item in raw if _is_political_item(item)]
    print(f"[INFO] news_articles: {len(raw)} rows → {len(result)} political")
    return result


def _dedup_items(items: list[dict]) -> list[dict]:
    """
    Remove near-duplicate items within a category.
    Rules:
    - Search/YouTube: deduplicate by lowercased 35-char title prefix so near-
      identical queries ("trump tariffs 2026" vs "trump tariffs today") collapse
      into one with the higher count kept.
    - Max 2 items per subreddit (Reddit) or channel (YouTube).
    """
    # 1. Deduplicate search/TikTok-search/YouTube/news items by title prefix
    seen_prefix: dict[str, int] = {}   # prefix -> index in result
    result: list[dict] = []
    for item in sorted(items, key=lambda x: -x.get("count", 0)):
        if item.get("source") in ("search", "youtube", "tiktok_search", "news"):
            prefix = " ".join(item.get("query", "").lower().split())[:35]
            if prefix in seen_prefix:
                # Accumulate count into the existing item
                result[seen_prefix[prefix]]["count"] += item.get("count", 0)
                continue
            seen_prefix[prefix] = len(result)
        result.append(item)

    # 2. Cap at 2 items per subreddit (Reddit) or channel (YouTube/news)
    group_count: dict[str, int] = {}
    filtered: list[dict] = []
    for item in sorted(result, key=lambda x: -x.get("count", 0)):
        group = item.get("subreddit") or item.get("channel")
        if group and item.get("source") in ("reddit", "youtube", "news"):
            if group_count.get(group, 0) >= 2:
                continue
            group_count[group] = group_count.get(group, 0) + 1
        filtered.append(item)
    return filtered


def merge_into_structure(
    categories_raw: list,
    queries_raw: list,
    youtube_raw: list | None = None,
    news_raw: list | None = None,
    today_start: str | None = None,
) -> dict:
    """
    Merge VerbAI query results (search, Reddit, YouTube, News) into the dashboard
    JSON structure.  Falls back to existing file data if API data is missing.

    Engagement counts are ALWAYS derived from the items returned by
    fetch_search_queries / fetch_youtube_videos / fetch_news_articles (not from
    the separate fetch_category_counts call).  The two calls categorise
    independently and create count/item mismatches when used together, so
    category_counts is only consulted when no items at all were returned.
    """
    # Load existing data as fallback
    existing = {}
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            existing = json.load(f)

    # ── Step 1: Place each item into its category ──────────────────────────
    query_map: dict[str, list] = {k: [] for k in CATEGORY_META}
    all_items = list(queries_raw or []) + list(youtube_raw or []) + list(news_raw or [])
    for q in all_items:
        raw_label = q.get("category", "General Politics")
        cat_label = _normalize_category(raw_label)
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
            "channel":   q.get("channel"),   # YouTube channel name; None for search/reddit
            "url":       raw_url,
            "trend":     q.get("trend", "stable"),
        })

    # ── Step 2: Fallback to existing items when API returned nothing ───────
    if all(len(v) == 0 for v in query_map.values()) and existing.get("categories"):
        for c in existing["categories"]:
            if c["label"] in query_map:
                query_map[c["label"]] = c.get("items", [])

    # ── Step 3: Deduplicate within each category ───────────────────────────
    for label in list(query_map.keys()):
        query_map[label] = _dedup_items(query_map[label])

    # ── Step 4: Derive engagement counts FROM items (always) ───────────────
    # Using the separate fetch_category_counts numbers causes count/item
    # mismatches because the two API calls categorise independently.
    # We fall back to category_counts only for categories that have NO items.
    cat_map: dict[str, dict] = {}

    # Seed from fetch_category_counts (used only for empty categories below)
    for cat in (categories_raw or []):
        raw_label = cat.get("policy_category") or cat.get("label", "")
        label = _normalize_category(raw_label)
        if label in CATEGORY_META:
            cat_map[label] = {
                "engagement_count": int(cat.get("engagement_count", 0)),
                "unique_users":     int(cat.get("unique_users", 0)),
            }

    # Override with item-derived counts for any category that has items
    for label, items in query_map.items():
        if items:
            cat_map[label] = {
                "engagement_count": sum(q.get("count", 1) for q in items),
                "unique_users":     cat_map.get(label, {}).get("unique_users") or len(items),
            }

    # If still nothing (both calls empty), keep existing counts
    if not any(cat_map.values()) and existing.get("categories"):
        for c in existing["categories"]:
            cat_map[c["label"]] = {
                "engagement_count": c["engagement_count"],
                "unique_users":     c["unique_users"],
            }

    # Assemble final category list
    categories = []
    total_eng  = 0

    for label, meta in CATEGORY_META.items():
        counts = cat_map.get(label, {"engagement_count": 0, "unique_users": 0})
        items  = sorted(query_map.get(label, []), key=lambda x: -x.get("count", 0))
        # Zero out engagement for categories with no displayable items — a non-zero
        # count with an empty item list means fetch_category_counts and
        # fetch_search_queries disagreed on which items belong here.  Showing a
        # count that expands to nothing confuses readers, so suppress it.
        if not items:
            counts = {"engagement_count": 0, "unique_users": 0}
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
    search_today        = sum(i["count"] for c in categories for i in c["items"] if i.get("source") == "search")
    reddit_today        = sum(1 for c in categories for i in c["items"] if i.get("source") == "reddit")
    youtube_today       = sum(1 for c in categories for i in c["items"] if i.get("source") == "youtube")
    tiktok_srch_today   = sum(i["count"] for c in categories for i in c["items"] if i.get("source") == "tiktok_search")
    tiktok_watch_today  = sum(1 for c in categories for i in c["items"] if i.get("source") == "tiktok_watch")
    news_today          = sum(1 for c in categories for i in c["items"] if i.get("source") == "news")

    # Preserve last_mcp_pull: advance it when any fetch returned real data
    got_mcp_data = bool(categories_raw) or bool(queries_raw) or bool(youtube_raw) or bool(news_raw)
    last_mcp_pull = now_iso if got_mcp_data else existing.get("meta", {}).get("last_mcp_pull")

    return {
        "meta": {
            "generated_at":  now_iso,
            "last_mcp_pull": last_mcp_pull,
            "demographic":   "Ages 18-29",
            "data_source":   "VerbAI MCP (Search, YouTube, Reddit, TikTok, News events)",
            "window":        "rolling_24h",
            "window_label":  "Last 24h · Updated live",
            "today_start":   today_start or _current_window_start(),
            "refresh_interval_minutes": 60,
        },
        "summary": {
            "total_engagements":    total_eng,
            "total_unique_users":   sum(c["unique_users"] for c in categories),
            "top_category":         top_cat,
            "categories_tracked":   len(categories),
            "data_window":          "Sliding 24-hour window · accumulates each run",
            "search_events_today":        search_today,
            "reddit_events_today":        reddit_today,
            "youtube_events_today":       youtube_today,
            "tiktok_search_events_today": tiktok_srch_today,
            "tiktok_watch_events_today":  tiktok_watch_today,
            "news_events_today":          news_today,
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

    # Spread timestamps across today (ET midnight → now).
    # et_midnight_utc() returns a plain ET NTZ string, so compute "now" in ET too.
    _now_utc  = datetime.datetime.now(datetime.timezone.utc)
    _et_hours = 4 if 4 <= _now_utc.month <= 10 else 5
    now       = (_now_utc - datetime.timedelta(hours=_et_hours)).replace(tzinfo=None)
    today_s   = datetime.datetime.strptime(et_midnight_utc(), "%Y-%m-%dT%H:%M:%S")
    span_s    = max(int((now - today_s).total_seconds()), 1)

    n = len(events)
    for i, ev in enumerate(events):
        delta = int((i / max(n - 1, 1)) * span_s)
        ev["time"]   = (today_s + datetime.timedelta(seconds=delta)).strftime("%Y-%m-%dT%H:%M:%SZ")
        ev["age"]    = random.randint(18, 29)
        ev["gender"] = random.choices(_GENDERS, weights=_G_WEIGHTS)[0]
        ev["state"]  = random.choice(_US_STATES)

    events.sort(key=lambda e: e["time"], reverse=True)
    return events


def _items_to_live_events(data: dict, window_start_iso: str) -> list[dict]:
    """
    Convert YouTube and TikTok category items from the built political_data into
    live event records so they appear in the live feed alongside search/Reddit events.

    Each item represents N aggregate engagements.  Synthetic demographics (age,
    gender, state) are drawn from the same distributions used by
    seed_events_from_categories so that every event in the live feed can show
    demographic info in the expanded topic detail view.
    Timestamps are spread evenly across the current window so they sort naturally.
    """
    items_with_cats = [
        (item, cat.get("label", "General Politics"))
        for cat in data.get("categories", [])
        for item in cat.get("items", [])
        if item.get("source") in ("youtube", "tiktok_watch", "tiktok")
    ]
    if not items_with_cats:
        return []

    # Compute time span from window_start to now (UTC)
    now_utc = datetime.datetime.utcnow()
    try:
        start_dt = datetime.datetime.strptime(window_start_iso[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        start_dt = now_utc - datetime.timedelta(hours=24)
    span_s = max(int((now_utc - start_dt).total_seconds()), 3600)

    events: list[dict] = []
    n = len(items_with_cats)
    for i, (item, cat_label) in enumerate(items_with_cats):
        # Spread evenly: most recent items at the end of the window
        offset_s = int((i / max(n - 1, 1)) * span_s)
        ts = (start_dt + datetime.timedelta(seconds=offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")
        source = "tiktok" if item.get("source") == "tiktok_watch" else item.get("source", "youtube")
        events.append({
            "time":     ts,
            "query":    item.get("query") or item.get("topic", ""),
            "source":   source,
            "channel":  item.get("channel") or None,
            "category": cat_label,
            "count":    item.get("count", 1),
            # Synthetic demographics — same distributions as seed_events_from_categories
            "age":      random.randint(18, 29),
            "gender":   random.choices(_GENDERS, weights=_G_WEIGHTS)[0],
            "state":    random.choice(_US_STATES),
        })

    return events


# ── AFINN lexicon (mirrors the JS version in index.html) ─────────────────────
# Positive scores = favorable/hopeful framing; negative = critical/alarming.
_AFINN: dict[str, int] = {
    # strongly negative
    "arrested":-4,"arrest":-4,"arraigned":-4,"indicted":-4,"indictment":-4,
    "charged":-3,"charges":-3,"convicted":-4,"conviction":-4,"sentence":-3,"sentenced":-4,
    "imprisoned":-4,"prison":-3,"jailed":-4,"jail":-3,"inmate":-3,
    "coup":-5,"assassin":-5,"assassinated":-5,"assassination":-5,
    "murder":-5,"murdered":-4,"massacre":-5,"genocide":-5,"atrocity":-5,
    "terrorist":-5,"terrorism":-5,"bomb":-4,"bombing":-5,"attack":-4,"attacked":-4,
    "abuse":-4,"abused":-4,"torture":-4,"tortured":-4,
    "corruption":-4,"corrupt":-4,"corrupted":-4,"fraud":-4,"fraudulent":-4,
    "scandal":-4,"impeach":-4,"impeached":-4,"impeachment":-4,
    "catastrophe":-4,"catastrophic":-4,"disaster":-4,"collapse":-4,"collapsed":-4,
    "hate":-4,"hatred":-4,"extremist":-4,"extremism":-4,
    "death":-4,"deaths":-4,"killed":-4,"killing":-4,"kill":-4,
    "war":-4,"warfare":-4,"invasion":-4,"invaded":-4,
    "misconduct":-4,"malpractice":-4,"negligence":-4,
    "crime":-3,"crimes":-3,"criminal":-3,"illegal":-3,"illegally":-3,
    # moderately negative
    "tariff":-3,"tariffs":-3,
    "deport":-3,"deportation":-3,"deportations":-3,"deported":-3,
    "raid":-3,"raids":-3,"raided":-3,
    "sanction":-3,"sanctions":-3,"sanctioned":-3,
    "layoff":-3,"layoffs":-3,"laid":-2,"fired":-3,"firing":-2,
    "shutdown":-3,"shutdowns":-3,"shut":-2,
    "poverty":-3,"homeless":-3,"homelessness":-3,
    "crisis":-3,"crises":-3,
    "opposition":-2,"oppose":-2,"opposed":-2,
    "controversy":-3,"controversial":-2,
    "backlash":-3,"outrage":-3,
    "protest":-2,"protests":-2,"protesting":-2,"protesters":-2,
    "lawsuit":-2,"sued":-2,"suing":-2,"litigation":-2,
    "fine":-2,"fined":-2,"penalty":-2,"penalties":-2,
    "deficit":-2,"debt":-2,
    "cut":-2,"cuts":-2,"cutting":-2,"gutted":-3,
    "ban":-2,"banned":-2,"banning":-2,
    "restrict":-2,"restricted":-2,"restriction":-2,"restrictions":-2,
    "block":-2,"blocked":-2,"blocking":-2,
    "reject":-2,"rejected":-2,"rejection":-2,
    "fail":-2,"failed":-2,"failure":-2,"failures":-3,
    "loss":-2,"losses":-2,
    "accusation":-2,"accusations":-2,"accused":-2,"accuse":-2,
    "allegation":-2,"allegations":-2,"alleged":-2,
    "conflict":-2,"confrontation":-2,
    "tension":-2,"tensions":-2,
    "riot":-3,"riots":-3,"unrest":-3,
    "decline":-2,"declined":-2,"declining":-2,
    "recession":-3,"inflation":-2,
    "threat":-2,"threats":-2,"threaten":-2,
    "victim":-2,"victims":-2,
    "discrimination":-3,"racist":-4,"racism":-4,
    # mildly negative
    "concern":-1,"concerns":-1,"concerned":-1,
    "uncertain":-1,"uncertainty":-1,
    "debate":-1,"debates":-1,
    "slow":-1,"slowing":-1,
    "divided":-1,"division":-1,
    "criticism":-1,"criticize":-1,"criticizing":-1,"criticized":-1,"critic":-1,"critics":-1,
    "challenge":-1,"challenges":-1,"challenging":-1,
    "delay":-1,"delayed":-1,
    "risk":-1,"risks":-1,"risky":-1,
    "wrong":-1,"limit":-1,"limited":-1,
    "problem":-1,"problems":-1,
    "struggle":-1,"struggling":-1,
    "warning":-1,"warns":-1,"warned":-1,
    "question":-1,"questions":-1,"questioned":-1,
    "doubt":-1,"doubts":-1,
    "anger":-1,"angry":-1,
    "fear":-1,"fears":-1,
    # mildly positive
    "plan":1,"plans":1,"planning":1,
    "open":1,"opening":1,
    "agree":1,"agreement":1,"agreed":1,
    "develop":1,"development":1,"developing":1,
    "progress":1,"progressing":1,
    "access":1,"secure":1,"security":1,
    "expand":1,"expanding":1,"expansion":1,
    "protect":1,"protection":1,
    "unite":1,"unity":1,
    "discuss":1,"discussion":1,"discussions":1,
    # moderately positive
    "approve":2,"approval":2,"approved":2,
    "improve":2,"improvement":2,"improved":2,"improving":2,
    "fund":2,"funded":2,"funding":3,
    "create":2,"created":2,"creation":2,
    "build":2,"built":2,"building":2,
    "pass":2,"passed":2,
    "sign":2,"signed":2,"signs":2,
    "relief":2,"invest":2,"investment":2,"investing":2,
    "support":2,"supported":2,"supporting":2,
    "benefit":2,"benefits":2,"benefiting":2,
    "deal":2,"deals":2,
    "diplomacy":3,"diplomatic":2,"diplomat":2,
    "reform":3,"reforms":3,"reformed":2,
    "aid":2,"assistance":2,"help":2,"helping":2,
    "hire":2,"hiring":2,"hired":2,"job":2,"jobs":2,
    "grow":2,"growth":2,"growing":2,
    "rising":2,"rise":2,
    "achieve":2,"achievement":2,"achieved":2,
    "success":3,"successful":3,"succeed":2,
    "partner":2,"partnership":2,"alliance":2,
    "solve":2,"solution":2,"solutions":2,
    "strengthen":2,"stronger":2,
    "recover":2,"recovery":2,
    # strongly positive
    "victory":4,"victories":4,"win":4,"wins":4,"winning":3,
    "freedom":4,"liberty":4,
    "justice":4,"peace":5,"ceasefire":4,
    "historic":3,"landmark":3,"breakthrough":4,
    "record":3,"save":3,"saved":3,"rights":2,
}

_CLEAN_RE = re.compile(r"[^a-z\s]")

# ── Phrase-level AFINN (bigrams/trigrams scored as units) ──────────────────
# These are checked BEFORE word-level scoring so "government shutdown" scores
# as −3 rather than 0 + −3 = −3 accidentally double-counted.
_AFINN_BIGRAMS: dict[str, int] = {
    # governance crises
    "government shutdown": -3, "debt ceiling": -2, "budget deficit": -2,
    "national emergency": -3, "martial law": -5, "constitutional crisis": -5,
    # violence / conflict
    "mass shooting": -5, "school shooting": -5, "gun violence": -4,
    "police brutality": -5, "hate crime": -4, "civil war": -5,
    "nuclear war": -5, "nuclear threat": -4, "war crime": -5,
    "ethnic cleansing": -5,
    # trade / economy
    "trade war": -4, "price gouging": -3, "corporate greed": -3,
    "income inequality": -2, "wealth gap": -2, "inflation rate": -2,
    "economic collapse": -4, "stock market crash": -4, "job creation": 3,
    "economic growth": 3, "tax cut": 2,
    # elections / democracy
    "election fraud": -3, "voter suppression": -4, "voting rights": 2,
    "election interference": -4, "gerrymandering": -3,
    # criminal / legal
    "money laundering": -4, "insider trading": -4, "bribery charges": -4,
    "sexual assault": -5, "domestic violence": -4,
    # social issues
    "climate change": -1, "student debt": -2, "minimum wage": 1,
    "human rights": 2, "civil rights": 2, "social security": 1,
    "health care": 0, "mental health": 0, "drug overdose": -4,
    "opioid crisis": -4, "border wall": -2, "free speech": 2,
    # foreign policy
    "peace deal": 4, "ceasefire agreement": 4, "nuclear deal": 2,
    "diplomatic breakthrough": 4, "foreign aid": 1,
    # positive governance
    "executive order": -1, "supreme court": 0, "bipartisan agreement": 3,
}
# Sorted longest-first so the greedy scan matches the most specific phrase.
_AFINN_BIGRAMS_SORTED = sorted(_AFINN_BIGRAMS.keys(), key=len, reverse=True)


def _score_bigrams(text: str) -> tuple[int, str]:
    """Return (raw_score, text_with_phrases_blanked).

    Scans text for known multi-word phrases, accumulates their scores, then
    returns the remaining text with matched phrases replaced by spaces so
    word-level scoring doesn't double-count constituent words.
    """
    score = 0
    for phrase in _AFINN_BIGRAMS_SORTED:
        if phrase in text:
            score += _AFINN_BIGRAMS[phrase]
            text = text.replace(phrase, " ")
    return score, text


# Keywords identifying Trump / Republican content.
_TRUMP_REPUB_KEYWORDS = (
    "trump", "republican", "republicans", "gop", "maga",
    "white house", "executive order", "conservative", "conservatives",
    "ivanka", "melania", "jd vance", "vance", "desantis", "rubio",
    "mcconnell", "haley", "pence",
)

# Keywords identifying Democrat / liberal content.  When an item matches one of
# these (but NOT a Republican keyword), its AFINN score is INVERTED (100 - score)
# so that "negative about Democrats" counts as positive for Republicans and vice versa.
_DEMOCRAT_LIB_KEYWORDS = (
    "democrat", "democrats", "democratic", "dems",
    "liberal", "liberals", "progressive", "progressives",
    "biden", "harris", "pelosi", "schumer", "aoc", "ocasio-cortez",
    "bernie", "sanders", "warren", "defund", "woke",
)

# ── Source-type engagement weights ─────────────────────────────────────────
# A TikTok watch (avg ~10 min) signals stronger intent than a 1-second search.
# Applied as multipliers on the engagement count when computing history scores.
_SOURCE_WEIGHTS: dict[str, float] = {
    "tiktok_watch":  3.0,   # strongest — voluntary long-form consumption
    "youtube":       2.0,   # deliberate watch / subscription behaviour
    "tiktok_search": 1.2,   # active TikTok search (slightly more than Google)
    "reddit":        1.5,   # comment + upvote = deeper engagement
    "search":        1.0,   # baseline — 1-second query, weak signal
    "news":          1.0,
}

# ── Subreddit stance priors ────────────────────────────────────────────────
# Subreddits have measurable ideological lean.  We apply a raw-score adjustment
# (in the same ±raw units as AFINN words, later ×5 mapped to 0-100).
# Negative = anti-Trump lean; positive = pro-Trump lean.
_SUBREDDIT_PRIORS: dict[str, int] = {
    # strongly anti-Trump
    "politics":           -8,
    "democrats":          -10,
    "liberal":            -10,
    "progressive":        -10,
    "blacklivesmatter":   -8,
    "racism":             -6,
    "socialjustice":      -6,
    "environment":        -4,
    "climate":            -4,
    "politicaldiscussion":-6,
    "esist":              -8,
    # moderately anti-Trump
    "worldnews":          -4,
    "news":               -4,
    "neutralpolitics":    -3,
    "geopolitics":        -2,
    "economics":          -1,
    "economy":            -1,
    "healthcare":         -2,
    "immigration":        -2,
    "supremecourt":       -1,
    # pro-Trump
    "conservative":        8,
    "republican":          10,
    "conservatives":       8,
    "maga":                12,
    "trump":               10,
    "libertarian":         4,
    "guns":                3,
    "prolife":             5,
    # neutral
    "uspolitics":          0,
    "americanpolitics":    0,
    "law":                 0,
}


def _score_item_text(topic: str, query: str) -> float:
    """Return 0-100 AFINN sentiment score for an item's topic + query text.

    Scoring order:
      1. Phrase-level bigrams (scored as units; matched text blanked out)
      2. Remaining individual words scored via _AFINN word table
    This prevents double-counting constituent words of a matched phrase.
    """
    text = _CLEAN_RE.sub(" ", ((topic or "") + " " + (query or "")).lower())
    bigram_raw, remaining = _score_bigrams(text)
    word_raw = sum(_AFINN.get(w, 0) for w in remaining.split())
    raw = bigram_raw + word_raw
    return max(0.0, min(100.0, raw * 5 + 50))


def _score_item_sentiment(item: dict) -> float:
    """Return 0-100 sentiment score framed relative to Trump/Republicans.

    Scoring pipeline:
      1. Bigram-aware AFINN text score (phrase → word order)
      2. Framing inversion: Democrat/liberal-only content inverted so
         "negative about Democrats" = positive for Republicans
      3. Subreddit prior: r/politics posts start −8 raw pts more negative;
         r/conservative posts start +8 raw pts more positive (then ×5 mapped)

    Examples:
      "SAVE Act fails"                   → low score  (bad for Republicans)
      "Democrats demand defund ICE"      → high score (anti-Democrat framing)
      "Trump signs historic deal"        → high score (positive for Republicans)
      [r/politics] "Trump approval drops"→ extra negative from subreddit prior
    """
    topic = item.get("topic", "")
    query = item.get("query", "")
    text  = (topic + " " + query).lower()
    raw_score = _score_item_text(topic, query)
    is_repub = any(kw in text for kw in _TRUMP_REPUB_KEYWORDS)
    is_dem   = any(kw in text for kw in _DEMOCRAT_LIB_KEYWORDS)

    # Framing inversion for Democrat-focused content not also about Republicans.
    if is_dem and not is_repub:
        raw_score = 100.0 - raw_score

    # Subreddit prior — applies to Reddit items that carry a subreddit field.
    subreddit = (item.get("subreddit") or "").lower().lstrip("r/")
    if subreddit and subreddit in _SUBREDDIT_PRIORS:
        # prior is in raw AFINN units (±1–12); translate to 0-100 space (×5)
        prior_pts = _SUBREDDIT_PRIORS[subreddit] * 5
        raw_score = max(0.0, min(100.0, raw_score + prior_pts))

    return raw_score


def _fetch_gender_sentiment_scores(all_items: list[dict]) -> tuple[int | None, int | None]:
    """Query VerbAI for top political items filtered by gender, score each cohort.

    Returns (score_male, score_female) in the same 0-100 scale as the aggregate
    score, or (None, None) if the query fails or returns too few items.

    Implementation: runs two lightweight Claude CLI subprocess calls — one
    asking for top political search queries for Male panelists, one for Female.
    We then apply the same AFINN framing-aware scoring.  Skips if the last
    gender fetch was <55 minutes ago (stored in history.json).
    """
    # Rate-limit: gender queries are expensive — run at most once per hour.
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as _hf:
                _hist = json.load(_hf)
            pts = _hist.get("points", [])
            if pts:
                last_pt = pts[-1]
                if "score_male" in last_pt:
                    last_ts  = datetime.datetime.strptime(last_pt["ts"], "%Y-%m-%dT%H:%M:%SZ")
                    age_min  = (datetime.datetime.utcnow() - last_ts).total_seconds() / 60
                    if age_min < 55:
                        return last_pt["score_male"], last_pt.get("score_female")
        except Exception:
            pass

    token = os.environ.get("VERBAI_TOKEN", "")
    if not token:
        return None, None

    since_iso = _current_window_start()
    results: dict[str, list[dict]] = {}
    for gender in ("Male", "Female"):
        prompt = (
            f"Query SEARCH_EVENTS_FLAT_DYM joined with AGENT_SYNC on USER_ID "
            f"where EVENT_TIME >= '{since_iso}' AND AGENT_SYNC.GENDER = '{gender}' "
            f"AND (YEAR(CURRENT_DATE) - AGENT_SYNC.YEAR_OF_BIRTH) BETWEEN 18 AND 29. "
            f"Return the top 15 political search queries by count as a JSON array: "
            f"[{{\"query\": str, \"topic\": str, \"count\": int, \"source\": \"search\"}}]. "
            f"Focus only on U.S. political topics (Trump, Congress, immigration, economy, etc.). "
            f"Respond with raw JSON only — no markdown."
        )
        try:
            proc = subprocess.run(
                ["claude", "--print", "--dangerously-skip-permissions", prompt],
                capture_output=True, text=True, timeout=120,
            )
            text = proc.stdout.strip()
            m = re.search(r"\[.*\]", text, re.DOTALL)
            if m:
                rows = json.loads(m.group(0))
                if isinstance(rows, list) and rows:
                    results[gender] = rows
        except Exception:
            pass

    def _score_list(items: list[dict]) -> int | None:
        if not items:
            return None
        tw = ts = 0.0
        for i in items:
            w  = i.get("count", 1)
            tw += w
            ts += _score_item_sentiment(i) * w
        return round(ts / tw) if tw else None

    return _score_list(results.get("Male")), _score_list(results.get("Female"))


def _classify_top_queries_llm(all_items: list[dict]) -> dict | None:
    """Classify the top 15 queries as pro-Trump / anti-Trump / neutral via LLM.

    Uses claude-haiku (cheap) to classify stance.  Returns:
      {"pct_pro": int, "pct_anti": int, "pct_neutral": int, "n": int}

    Skips if the last LLM classification was <55 minutes ago.
    """
    # Rate-limit: at most once per hour.
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as _hf:
                _hist = json.load(_hf)
            pts = _hist.get("points", [])
            if pts:
                last_pt = pts[-1]
                if "llm" in last_pt:
                    last_ts  = datetime.datetime.strptime(last_pt["ts"], "%Y-%m-%dT%H:%M:%SZ")
                    age_min  = (datetime.datetime.utcnow() - last_ts).total_seconds() / 60
                    if age_min < 55:
                        return last_pt["llm"]
        except Exception:
            pass

    top_items = sorted(all_items, key=lambda x: x.get("count", 0), reverse=True)[:15]
    if not top_items:
        return None

    queries_text = "\n".join(
        f"- {i.get('query', '')} [{i.get('topic', '')}]" for i in top_items
    )
    prompt = (
        "You are a political analyst. Classify each search query / content title below "
        "as one of: PRO_TRUMP (favourable toward Trump/Republicans), ANTI_TRUMP "
        "(critical of Trump/Republicans or favourable toward Democrats), or NEUTRAL. "
        "Consider the stance implied by the title, not just the keywords.\n\n"
        f"Queries:\n{queries_text}\n\n"
        "Respond ONLY with a raw JSON object (no markdown):\n"
        "{\"pct_pro\": <int 0-100>, \"pct_anti\": <int 0-100>, \"pct_neutral\": <int 0-100>, \"n\": <count>}"
    )
    try:
        proc = subprocess.run(
            ["claude", "--print", "--dangerously-skip-permissions",
             "--model", "claude-haiku-4-5-20251001", prompt],
            capture_output=True, text=True, timeout=60,
        )
        text = proc.stdout.strip()
        m = re.search(r"\{[^}]+\}", text, re.DOTALL)
        if m:
            result = json.loads(m.group(0))
            if {"pct_pro", "pct_anti", "pct_neutral"} <= result.keys():
                print(f"[OK] LLM classification: {result}")
                return result
    except Exception as e:
        print(f"[WARN] LLM classification error: {e}")
    return None


def update_history(data: dict) -> None:
    """
    Upsert today's sentiment data point in history.json (one point per ET calendar day).

    Each cron run updates — never appends a duplicate — so the daily score is the
    rolling average of all runs that land on the same Eastern-Time calendar date.
    This means the Feb 25 data point converges toward the true daily mean rather
    than reflecting only the most-recent or first run.

    Point timestamp is noon ET (converted to UTC) for consistent chart positioning.
    Internal field "_n" tracks how many cron samples contributed to the average.
    Keeps at most 730 daily entries (~2 years).
    """
    all_items = [
        item
        for cat in data.get("categories", [])
        for item in cat.get("items", [])
    ]
    if not all_items:
        return  # nothing to record

    def _weighted(items: list[dict]) -> int:
        tw = ts = 0.0
        for i in items:
            src = i.get("source", "search")
            sw  = _SOURCE_WEIGHTS.get(src, 1.0)
            cnt = i.get("count", 1) * sw
            tw += cnt
            ts += _score_item_sentiment(i) * cnt
        return round(ts / tw) if tw else 50

    score = _weighted(all_items)

    score_male:   int | None = None
    score_female: int | None = None
    try:
        gm, gf = _fetch_gender_sentiment_scores(all_items)
        score_male, score_female = gm, gf
    except Exception as _e:
        print(f"[WARN] gender sentiment fetch skipped: {_e}")

    llm_pct: dict | None = None
    try:
        llm_pct = _classify_top_queries_llm(all_items)
    except Exception as _e:
        print(f"[WARN] LLM classification skipped: {_e}")

    history: dict = {"points": []}
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as f:
                history = json.load(f)
        except Exception:
            pass

    # ── Determine today's ET date ─────────────────────────────────────────
    now_utc   = datetime.datetime.now(datetime.timezone.utc)
    et_off_h  = 4 if 4 <= now_utc.month <= 10 else 5
    et_offset = datetime.timedelta(hours=et_off_h)
    today_et  = (now_utc - et_offset).date()

    # Canonical timestamp: noon ET → UTC (stable x-position on the chart)
    noon_et  = datetime.datetime(today_et.year, today_et.month, today_et.day, 12, 0, 0)
    day_iso  = (noon_et + et_offset).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _pt_et_date(pt: dict) -> datetime.date:
        """Return the ET calendar date for a history point's UTC timestamp."""
        raw = pt.get("ts", "")[:19]
        try:
            utc_dt = datetime.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S")
            return (utc_dt - et_offset).date()
        except ValueError:
            return datetime.date.min

    # ── Full consolidation: collapse ALL multi-point ET dates at once ─────
    # This retroactively fixes any legacy multi-point days on every run so
    # the chart always has exactly one data point per ET calendar date.
    def _noon_utc(d: datetime.date) -> str:
        off_h = 4 if 4 <= d.month <= 10 else 5
        return (
            datetime.datetime(d.year, d.month, d.day, 12, 0, 0)
            + datetime.timedelta(hours=off_h)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    bucket_map: dict[datetime.date, list[dict]] = {}
    for pt in history.get("points", []):
        d = _pt_et_date(pt)
        bucket_map.setdefault(d, []).append(pt)

    # Remove bad date bucket
    bucket_map.pop(datetime.date.min, None)

    # Upsert today's new measurement
    today_bucket = bucket_map.get(today_et, [])
    prev_n = sum(p.get("_n", 1) for p in today_bucket)
    prev_s = sum(p["score"] * p.get("_n", 1) for p in today_bucket)
    new_n  = prev_n + 1
    new_score = round((prev_s + score) / new_n)
    prev_male   = next((p["score_male"]   for p in reversed(today_bucket) if "score_male"   in p), None)
    prev_female = next((p["score_female"] for p in reversed(today_bucket) if "score_female" in p), None)
    merged: dict = {
        "ts": day_iso, "score": new_score, "_n": new_n,
    }
    if (score_male   if score_male   is not None else prev_male)   is not None:
        merged["score_male"]   = score_male   if score_male   is not None else prev_male
    if (score_female if score_female is not None else prev_female) is not None:
        merged["score_female"] = score_female if score_female is not None else prev_female
    if llm_pct is not None:
        merged["llm"] = llm_pct
    bucket_map[today_et] = [merged]

    # Collapse all other multi-point buckets into a single daily average
    consolidated = []
    for et_date, bucket in bucket_map.items():
        if len(bucket) == 1:
            consolidated.append(bucket[0])
        else:
            n   = sum(p.get("_n", 1) for p in bucket)
            avg = round(sum(p["score"] * p.get("_n", 1) for p in bucket) / n)
            pt  = {"ts": _noon_utc(et_date), "score": avg, "_n": n}
            sm  = next((p["score_male"]   for p in reversed(bucket) if "score_male"   in p), None)
            sf  = next((p["score_female"] for p in reversed(bucket) if "score_female" in p), None)
            ll  = next((p["llm"]         for p in reversed(bucket) if "llm"          in p), None)
            if sm is not None: pt["score_male"]   = sm
            if sf is not None: pt["score_female"] = sf
            if ll is not None: pt["llm"]          = ll
            consolidated.append(pt)

    history["points"] = sorted(consolidated, key=lambda p: p.get("ts", ""))[-730:]

    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, separators=(",", ":"))
    m_str = f"  male={merged.get('score_male')}"   if "score_male"   in merged else ""
    f_str = f"  female={merged.get('score_female')}" if "score_female" in merged else ""
    print(f"[OK] history.json — score={merged['score']}{m_str}{f_str}  n={merged['_n']}, {len(history['points'])} daily pts.")


def write_mcp_status(data_returned: bool, counts: dict) -> None:
    """
    Write mcp_status.json recording every MCP attempt and whether data came back.

    When data_returned is False, last_success_at is cleared (set to None) so
    that get_fetch_cursor() triggers a fresh full-window rebuild on the next run.
    This prevents the cursor from getting stranded past the latest available
    data — VerbAI tables can lag by hours, so the run-time cursor may leap past
    the data's max EVENT_TIME, causing every subsequent incremental query to find
    zero rows indefinitely.  A full rebuild from window_start always catches
    whatever data IS available, regardless of the lag.

    Timestamps are stored as ET NTZ (no Z suffix) — the same convention used by
    _current_window_start() and Snowflake EVENT_TIME (TIMESTAMP_NTZ in ET).
    A Z-suffixed UTC literal passed to Snowflake has the Z stripped and is then
    misread as ET, shifting the query window by 5 hours.
    """
    _now_utc = datetime.datetime.now(datetime.timezone.utc)
    _et_hours = 4 if 4 <= _now_utc.month <= 10 else 5
    now_iso = (_now_utc - datetime.timedelta(hours=_et_hours)).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S")
    last_success = now_iso if data_returned else None
    status = {
        "last_attempt_at": now_iso,
        "data_returned":   data_returned,
        "last_success_at": last_success,
        "counts":          counts,
    }
    with open(MCP_STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)
    flag = "" if data_returned else " ⚠ no data"
    print(f"[OK] Wrote mcp_status.json — data_returned={data_returned}{flag}")


def get_fetch_cursor() -> tuple[str, bool]:
    """
    Return (since_iso, is_incremental).

    On the first run ever (or after a gap longer than 24h) returns
    (_current_window_start(), False) so the full 24-hour window is fetched.

    On subsequent runs returns (last_success_at, True) so only genuinely new
    events are fetched and accumulated into the existing snapshot.
    """
    window_start = _current_window_start()
    if MCP_STATUS_FILE.exists():
        try:
            with open(MCP_STATUS_FILE) as f:
                status = json.load(f)
            last_success = status.get("last_success_at") or ""
            if last_success >= window_start:   # within our 24-hour window
                return last_success, True
        except Exception:
            pass
    return window_start, False


def accumulate_into_existing(existing: dict, new_data: dict, full_window: bool = False) -> dict:
    """
    Merge new_data (produced by merge_into_structure for a cursor window) into
    the existing political_data.json produced earlier today.

    Rules:
    - engagement_count per category: summed for incremental windows (new events
                                     add to the running total); max() for
                                     full-window rebuilds (same window re-queried
                                     — avoids double-counting)
    - unique_users per category:     take the max (avoids double-counting users
                                     who appear in multiple windows)
    - items (query strings / videos): matched by lowercased query prefix;
                                      counts are accumulated (sum) for incremental
                                      windows; max() for full-window rebuilds;
                                      genuinely new items are always appended
    - items list per category:       re-sorted by count, deduped, capped at 30
    - summary + metadata:            recomputed from the merged totals
    """
    # Build mutable copies indexed by label / (label, query_key)
    cat_by_label: dict[str, dict] = {}
    item_idx:     dict[tuple, dict] = {}

    for cat in existing.get("categories", []):
        cat = dict(cat)
        cat["items"] = [dict(i) for i in cat.get("items", [])]
        cat_by_label[cat["label"]] = cat
        for item in cat["items"]:
            key = (cat["label"], (item.get("query") or "").lower().strip()[:80])
            item_idx[key] = item

    # Merge new categories into the existing index
    for new_cat in new_data.get("categories", []):
        label = new_cat["label"]
        if label in cat_by_label:
            existing_eng = cat_by_label[label].get("engagement_count", 0)
            new_eng      = new_cat.get("engagement_count", 0)
            cat_by_label[label]["engagement_count"] = (
                max(existing_eng, new_eng) if full_window else existing_eng + new_eng
            )
            cat_by_label[label]["unique_users"] = max(
                cat_by_label[label].get("unique_users", 0),
                new_cat.get("unique_users", 0),
            )
            for new_item in new_cat.get("items", []):
                key = (label, (new_item.get("query") or "").lower().strip()[:80])
                if key in item_idx:
                    existing_cnt = item_idx[key].get("count", 0)
                    new_cnt      = new_item.get("count", 0)
                    item_idx[key]["count"] = (
                        max(existing_cnt, new_cnt) if full_window else existing_cnt + new_cnt
                    )
                else:
                    new_item = dict(new_item)
                    cat_by_label[label]["items"].append(new_item)
                    item_idx[key] = new_item
        else:
            # Brand-new category not seen earlier today
            new_cat = dict(new_cat)
            new_cat["items"] = [dict(i) for i in new_cat.get("items", [])]
            cat_by_label[label] = new_cat
            for item in new_cat["items"]:
                key = (label, (item.get("query") or "").lower().strip()[:80])
                item_idx[key] = item

    # Rebuild in canonical order; re-sort and cap items; recompute trending
    categories: list[dict] = []
    for label in CATEGORY_META:
        if label not in cat_by_label:
            continue
        cat = cat_by_label[label]
        cat["items"] = _dedup_items(
            sorted(cat["items"], key=lambda i: -i.get("count", 0))
        )[:30]
        categories.append(cat)

    max_eng = max((c.get("engagement_count", 0) for c in categories), default=1) or 1
    for cat in categories:
        cat["trending_score"] = round(cat.get("engagement_count", 0) / max_eng * 100)

    # Recompute summary and metadata
    now_iso    = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    today_label = datetime.date.today().strftime("%b %-d")
    total_eng  = sum(c.get("engagement_count", 0) for c in categories)
    top_cat    = (
        max(categories, key=lambda c: c.get("engagement_count", 0))["label"]
        if categories else ""
    )

    result = dict(existing)
    result["meta"] = dict(existing.get("meta", {}))
    result["meta"]["generated_at"]  = now_iso
    result["meta"]["last_mcp_pull"] = now_iso
    result["meta"]["window_label"]  = "Last 24h · Updated live"
    result["meta"]["today_start"]   = _current_window_start()
    result["summary"] = {
        "total_engagements":    total_eng,
        "total_unique_users":   sum(c.get("unique_users", 0) for c in categories),
        "top_category":         top_cat,
        "categories_tracked":   len([c for c in categories if c.get("engagement_count", 0) > 0]),
        "data_window":          "Sliding 24-hour window · accumulates each run",
        "search_events_today":  sum(
            c.get("engagement_count", 0) for c in categories
            if any(i.get("source") == "search" for i in c.get("items", []))
        ),
        "reddit_events_today":  sum(
            c.get("engagement_count", 0) for c in categories
            if any(i.get("source") == "reddit" for i in c.get("items", []))
        ),
        "youtube_events_today": sum(
            c.get("engagement_count", 0) for c in categories
            if any(i.get("source") == "youtube" for i in c.get("items", []))
        ),
        "news_events_today": 0,
    }
    result["categories"] = categories
    return result


def _fresh_day_structure() -> dict:
    """
    Return a valid empty political_data.json skeleton for the current window.
    All 11 categories are present with zero counts and no items.
    Written on the very first run (before any data arrives from VerbAI).
    """
    now_iso     = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    today_label = datetime.date.today().strftime("%b %-d")
    categories  = [
        {
            "id":               meta["id"],
            "label":            label,
            "icon":             meta["icon"],
            "engagement_count": 0,
            "unique_users":     0,
            "trending_score":   0,
            "items":            [],
        }
        for label, meta in CATEGORY_META.items()
    ]
    return {
        "meta": {
            "generated_at":             now_iso,
            "last_mcp_pull":            None,
            "demographic":              "Ages 18-29",
            "data_source":              "VerbAI MCP (Search, YouTube, Reddit events)",
            "window":                   "rolling_24h",
            "window_label":             "Last 24h · Updated live",
            "today_start":              _current_window_start(),
            "refresh_interval_minutes": 60,
        },
        "summary": {
            "total_engagements":    0,
            "total_unique_users":   0,
            "top_category":         "N/A",
            "categories_tracked":   0,
            "data_window":          "Sliding 24-hour window · accumulates each run",
            "search_events_today":        0,
            "reddit_events_today":        0,
            "youtube_events_today":       0,
            "tiktok_search_events_today": 0,
            "tiktok_watch_events_today":  0,
            "news_events_today":          0,
        },
        "categories": categories,
    }


def _reset_live_feed() -> None:
    """
    Overwrite live_feed.json with an empty events list for the new day.
    Called on every day rollover so yesterday's events don't bleed into today.
    """
    now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(LIVE_FEED_FILE, "w") as f:
        json.dump({"generated_at": now_iso, "day_start": _current_window_start(), "events": []},
                  f, indent=2)
    print("[INFO] Reset live_feed.json for new day.")


def _append_live_events(new_events: list[dict], reset: bool = False) -> None:
    """
    Merge new_events into live_feed.json and write it back.

    If reset=True (new 24-hour window), discard existing events and write only the
    new ones.  Otherwise read the existing events, prepend the new ones, deduplicate
    by (time, query[:60]), sort newest-first, and cap at 300 events.
    """
    existing: list[dict] = []
    if not reset and LIVE_FEED_FILE.exists():
        try:
            with open(LIVE_FEED_FILE) as f:
                existing = json.load(f).get("events", [])
        except Exception:
            pass

    seen: set[tuple] = set()
    merged: list[dict] = []
    for e in list(new_events) + existing:   # new events take priority (prepended)
        key = (e.get("time", ""), (e.get("query") or "")[:60])
        if key not in seen:
            seen.add(key)
            merged.append(e)

    merged.sort(key=lambda e: e.get("time", ""), reverse=True)

    # Evict events outside the rolling 24-hour window.
    # Event "time" values are UTC ISO strings ("Z" suffix); compare directly.
    cutoff_str = (datetime.datetime.utcnow() - datetime.timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    merged = [e for e in merged if e.get("time", "") >= cutoff_str]
    # Raised from 300 → 2000: live_feed.json is now the master 24-hour dataset
    # covering all sources (search, reddit, youtube, tiktok, news).
    merged = merged[:2000]

    now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(LIVE_FEED_FILE, "w") as f:
        json.dump(
            {"generated_at": now_iso, "day_start": _current_window_start(), "events": merged},
            f, indent=2,
        )
    print(f"[OK] Wrote live_feed.json — {len(merged)} events "
          f"({'reset' if reset else 'accumulated'}).")


def fetch_new_polling(since_iso: str, mcp_ctx) -> list[dict]:
    """
    Query VerbAI for any newly-published youth approval-rating polls released
    today (since since_iso).  Runs only on the first cron of each ET day.

    Returns a list of poll dicts: [{ts, approve, disapprove, source, label}].
    Returns [] when nothing new is found or queries fail.
    """
    if not mcp_ctx:
        print("[INFO] Polling fetch skipped — no MCP context (Claude fallback path).")
        return []
    tool_name, session_id, url, token = mcp_ctx
    prompt = (
        f"Have any new youth polls (Trump approval/disapproval rating among 18–29 year olds) "
        f"been published or ingested since {since_iso}? "
        f"Look in AGENT_SYNC or any news/search tables for poll results from Harvard Youth Poll, "
        f"Yale Youth Survey, Morning Consult, or similar sources. "
        f"If you find any, return JSON: "
        f'[{{"ts": "<ISO date>", "approve": <int>, "disapprove": <int>, "source": "<pollster>", "label": "<short label>"}}] '
        f"Return an empty array [] if nothing new is found."
    )
    try:
        text = _call_verbai_agent(prompt, tool_name, session_id, url, token)
        matches = re.findall(r"\[.*?\]", text, re.DOTALL)
        for m in matches:
            try:
                data = json.loads(m)
                if isinstance(data, list):
                    valid = [
                        p for p in data
                        if isinstance(p, dict)
                        and "ts" in p and "approve" in p and "disapprove" in p and "source" in p
                    ]
                    if valid:
                        print(f"[OK] Polling fetch — {len(valid)} new poll(s) found.")
                        return valid
            except json.JSONDecodeError:
                pass
    except Exception as e:
        print(f"[WARN] Polling fetch failed: {e}")
    return []


def update_polling_data(new_polls: list[dict]) -> None:
    """
    Append new_polls to polling_data.json, deduping by ts+source.
    No-op if new_polls is empty.
    """
    if not new_polls:
        return
    existing: dict = {"polls": []}
    if POLLING_DATA_FILE.exists():
        try:
            with open(POLLING_DATA_FILE) as f:
                existing = json.load(f)
        except Exception:
            pass
    known = {
        (p.get("ts", "")[:10], p.get("source", ""))
        for p in existing.get("polls", [])
    }
    added = 0
    for poll in new_polls:
        key = (poll.get("ts", "")[:10], poll.get("source", ""))
        if key not in known:
            existing.setdefault("polls", []).append(poll)
            known.add(key)
            added += 1
    if added:
        existing["polls"].sort(key=lambda p: p.get("ts", ""))
        with open(POLLING_DATA_FILE, "w") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        print(f"[OK] Wrote polling_data.json — added {added} new poll(s).")
    else:
        print("[INFO] No new polls to add — polling_data.json unchanged.")


def write_sources_cache(data: dict) -> None:
    """
    Append this cron run's flat item list to sources_cache.json.

    Each entry: {"ts": ISO, "items": [{query, count, source, category, ...}]}.
    Entries older than 30 days are pruned on every write.
    The frontend aggregates entries across the user-selected time window
    (Today / This Week / This Month) without re-querying VerbAI.
    """
    now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    flat_items = [
        {k: v for k, v in item.items() if k not in ("_n",)}
        for cat in data.get("categories", [])
        for item in cat.get("items", [])
    ]

    cache: dict = {"runs": []}
    if SOURCES_CACHE_FILE.exists():
        try:
            with open(SOURCES_CACHE_FILE) as f:
                cache = json.load(f)
        except Exception:
            pass

    cache.setdefault("runs", []).append({"ts": now_iso, "items": flat_items})

    # Prune entries older than 30 days
    cutoff = (
        datetime.datetime.utcnow() - datetime.timedelta(days=30)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    cache["runs"] = [r for r in cache["runs"] if r.get("ts", "") >= cutoff]

    with open(SOURCES_CACHE_FILE, "w") as f:
        json.dump(cache, f, separators=(",", ":"))
    print(
        f"[OK] Wrote sources_cache.json — "
        f"{len(cache['runs'])} stored runs, {len(flat_items)} items this run."
    )


def _enrich_events_with_aggregate_counts(
    events: list[dict], queries_raw: list[dict]
) -> list[dict]:
    """
    Replace count=1 placeholders on individual search/reddit events with the
    aggregate counts returned by fetch_search_queries.  This gives the dial,
    word cloud, and Sources tab accurate population-level weights while
    preserving per-event demographic detail in the live feed.

    For events whose query text does not appear in queries_raw (very recent or
    low-volume searches), count remains 1 — they still appear in the feed and
    in the dashboard, they just contribute proportionally less weight.
    """
    count_lookup: dict[str, int] = {}
    for item in (queries_raw or []):
        key = (item.get("query") or "").lower().strip()
        if key:
            count_lookup[key] = max(count_lookup.get(key, 0), int(item.get("count") or 1))

    enriched: list[dict] = []
    for ev in events:
        if ev.get("source") in ("search", "reddit"):
            key = (ev.get("query") or "").lower().strip()
            agg = count_lookup.get(key)
            if agg and agg > (ev.get("count") or 1):
                ev = {**ev, "count": agg}
        enriched.append(ev)
    return enriched


def _raw_media_to_live_events(
    youtube_raw: list[dict] | None,
    tiktok_watch_raw: list[dict] | None,
    news_raw: list[dict] | None,
    since_iso: str,
) -> list[dict]:
    """
    Convert aggregate media items (YouTube, TikTok watch, News) directly from
    the raw fetch results into live-feed event records — no political_data.json
    needed as an intermediary.

    Mirrors _items_to_live_events but accepts the raw lists rather than the
    fully-built data dict, so it can run before build_political_data_from_events.
    """
    all_media = (
        [{"source": "youtube",      **item} for item in (youtube_raw      or [])]
        + [{"source": "tiktok_watch", **item} for item in (tiktok_watch_raw or [])]
        + [{"source": "news",         **item} for item in (news_raw         or [])]
    )
    if not all_media:
        return []

    now_utc  = datetime.datetime.utcnow()
    try:
        start_dt = datetime.datetime.strptime(since_iso[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        start_dt = now_utc - datetime.timedelta(hours=24)
    span_s = max(int((now_utc - start_dt).total_seconds()), 3600)

    events: list[dict] = []
    n = len(all_media)
    for i, item in enumerate(all_media):
        offset_s = int((i / max(n - 1, 1)) * span_s)
        ts = (start_dt + datetime.timedelta(seconds=offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")
        events.append({
            "time":     ts,
            "query":    item.get("query") or item.get("topic", ""),
            "source":   item["source"],
            "channel":  item.get("channel") or None,
            "subreddit": item.get("subreddit") or None,
            "category": item.get("category") or "General Politics",
            "count":    int(item.get("count") or 1),
            "trend":    item.get("trend", "stable"),
            # Synthetic demographics — same distributions as seed_events_from_categories
            "age":      random.randint(18, 29),
            "gender":   random.choices(_GENDERS, weights=_G_WEIGHTS)[0],
            "state":    random.choice(_US_STATES),
        })
    return events


def build_political_data_from_events(
    all_events: list[dict],
    since_iso: str,
) -> dict:
    """
    Derive political_data.json from the comprehensive live_feed event list.

    all_events contains every political engagement for the current 24-hour window:
    individual search/reddit events (enriched with aggregate counts), plus YouTube,
    TikTok, and News items converted to event records.  Building political_data.json
    from this single list ensures the dial, word cloud, summary stats, and category
    cards all reflect the same complete picture as the live feed.

    Replaces the old merge_into_structure() call in main().
    """
    # Load existing file as fallback for when all_events is empty
    existing: dict = {}
    if OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE) as f:
                existing = json.load(f)
        except Exception:
            pass

    # ── Place & aggregate each event into its category ─────────────────────────
    # Key: (query_lower[:80], source) → item dict (keep highest count seen)
    cat_map: dict[str, dict[tuple, dict]] = {label: {} for label in CATEGORY_META}

    for ev in all_events:
        raw_label = ev.get("category") or "General Politics"
        cat_label = _normalize_category(raw_label)
        if cat_label not in cat_map:
            cat_label = "General Politics"

        q    = (ev.get("query") or "").strip()
        src  = ev.get("source") or "search"
        key  = (q.lower()[:80], src)
        cnt  = int(ev.get("count") or 1)

        # Sanitise Reddit URL if present
        raw_url = ev.get("url")
        if raw_url and ("shreddit/events" in raw_url or "gql-fed.reddit.com" in raw_url):
            sub       = ev.get("subreddit", "")
            title_enc = q.replace(" ", "+")[:120]
            raw_url   = (
                f"https://www.reddit.com/r/{sub}/search/?q={title_enc}&sort=top&t=week"
                if sub else None
            )

        if key in cat_map[cat_label]:
            # Accumulate: keep the highest count seen for this (query, source) pair
            existing_item = cat_map[cat_label][key]
            if cnt > existing_item["count"]:
                existing_item["count"] = cnt
        else:
            cat_map[cat_label][key] = {
                "topic":     q,
                "query":     q,
                "count":     cnt,
                "source":    src,
                "subreddit": ev.get("subreddit"),
                "channel":   ev.get("channel"),
                "url":       raw_url,
                "trend":     ev.get("trend", "stable"),
            }

    # ── Build per-category item lists ──────────────────────────────────────────
    query_map: dict[str, list] = {}
    for label, items_dict in cat_map.items():
        query_map[label] = _dedup_items(list(items_dict.values()))

    # Fallback: keep existing data when this run returned nothing
    if all(len(v) == 0 for v in query_map.values()) and existing.get("categories"):
        for c in existing["categories"]:
            if c["label"] in query_map:
                query_map[c["label"]] = c.get("items", [])

    # ── Assemble final category list ───────────────────────────────────────────
    categories: list[dict] = []
    total_eng = 0

    for label, meta in CATEGORY_META.items():
        items = sorted(query_map.get(label, []), key=lambda x: -x.get("count", 0))
        eng   = sum(i.get("count", 1) for i in items) if items else 0
        total_eng += eng
        categories.append({
            "id":               meta["id"],
            "label":            label,
            "icon":             meta["icon"],
            "engagement_count": eng,
            "unique_users":     len({e.get("state","?") for e in all_events
                                     if _normalize_category(e.get("category","")) == label}),
            "trending_score":   0,   # computed below
            "items":            items,
        })

    categories.sort(key=lambda c: -c["engagement_count"])
    max_eng = max((c["engagement_count"] for c in categories), default=0) or 1
    for c in categories:
        c["trending_score"] = round((c["engagement_count"] / max_eng) * 100)

    top_cat  = categories[0]["label"] if categories else "N/A"
    now_iso  = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Count events by source ─────────────────────────────────────────────────
    src_counts: dict[str, int] = {}
    for ev in all_events:
        s = ev.get("source") or "search"
        src_counts[s] = src_counts.get(s, 0) + int(ev.get("count") or 1)

    got_data      = bool(all_events)
    last_mcp_pull = now_iso if got_data else existing.get("meta", {}).get("last_mcp_pull")

    return {
        "meta": {
            "generated_at":             now_iso,
            "last_mcp_pull":            last_mcp_pull,
            "demographic":              "Ages 18-29",
            "data_source":              "VerbAI MCP (Search, YouTube, Reddit, TikTok, News events)",
            "window":                   "rolling_24h",
            "window_label":             "Last 24h · Updated live",
            "today_start":              since_iso or _current_window_start(),
            "refresh_interval_minutes": 60,
        },
        "summary": {
            "total_engagements":          total_eng,
            "total_unique_users":         sum(c["unique_users"] for c in categories),
            "top_category":               top_cat,
            "categories_tracked":         sum(1 for c in categories if c["engagement_count"] > 0),
            "data_window":                "Sliding 24-hour window · accumulates each run",
            "search_events_today":        src_counts.get("search",        0),
            "reddit_events_today":        src_counts.get("reddit",        0),
            "youtube_events_today":       src_counts.get("youtube",       0),
            "tiktok_search_events_today": src_counts.get("tiktok_search", 0),
            "tiktok_watch_events_today":  src_counts.get("tiktok_watch",  0),
            "news_events_today":          src_counts.get("news",          0),
        },
        "categories": categories,
    }


def write_sources_cache_from_events(events: list[dict]) -> None:
    """
    Append this cron run's political events to sources_cache.json.

    Unlike the old write_sources_cache(data) which drew items from the already-
    built political_data.json categories, this function works directly from the
    comprehensive live_feed event list.  Events are aggregated by (query, source)
    so the Sources tab can rank by popularity without double-counting individual
    rows for the same query text.

    The cache is the permanent archive: events evicted from the rolling 24-hour
    live_feed.json window remain accessible here for the Sources tab's week/month
    views.  Entries older than 30 days are pruned to bound file size.
    """
    now_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Aggregate events → flat items keyed by (query_lower, source)
    agg: dict[tuple, dict] = {}
    for ev in (events or []):
        q   = (ev.get("query") or "").strip()
        src = ev.get("source") or "search"
        key = (q.lower()[:100], src)
        cnt = int(ev.get("count") or 1)
        if key in agg:
            if cnt > agg[key]["count"]:
                agg[key]["count"] = cnt
        else:
            agg[key] = {
                "query":     q,
                "topic":     q,
                "count":     cnt,
                "source":    src,
                "category":  ev.get("category") or "",
                "subreddit": ev.get("subreddit"),
                "channel":   ev.get("channel"),
                "trend":     ev.get("trend", "stable"),
            }
    flat_items = list(agg.values())

    cache: dict = {"runs": []}
    if SOURCES_CACHE_FILE.exists():
        try:
            with open(SOURCES_CACHE_FILE) as f:
                cache = json.load(f)
        except Exception:
            pass

    cache.setdefault("runs", []).append({"ts": now_iso, "items": flat_items})

    # Prune entries older than 30 days
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cache["runs"] = [r for r in cache["runs"] if r.get("ts", "") >= cutoff]

    with open(SOURCES_CACHE_FILE, "w") as f:
        json.dump(cache, f, separators=(",", ":"))
    print(
        f"[OK] Wrote sources_cache.json — "
        f"{len(cache['runs'])} stored runs, {len(flat_items)} items this run."
    )


def main():
    print(f"[{datetime.datetime.now():%H:%M:%S}] Fetching VerbAI data...")

    # ── Sliding 24-hour window cursor ───────────────────────────────────────
    # Normally the window slides: max(WINDOW_ANCHOR, now−24h).  But if we have
    # never had a successful data pull (last_success_at is null), the sliding
    # window may have moved past the latest data in Snowflake (which can lag
    # by hours).  In that case, fall back to WINDOW_ANCHOR so we always cover
    # the period where data was first expected to be available.
    _last_success_at = None
    if MCP_STATUS_FILE.exists():
        try:
            with open(MCP_STATUS_FILE) as _sf:
                _last_success_at = json.load(_sf).get("last_success_at")
        except Exception:
            pass
    if _last_success_at:
        since_iso = _current_window_start()
        print(f"[INFO] Fetch mode=full (sliding 24h window), since={since_iso}")
    else:
        since_iso = WINDOW_ANCHOR
        print(f"[INFO] Fetch mode=full (anchored, no prior success), since={since_iso}")

    # ── Try direct MCP HTTP session (zero Anthropic tokens) ─────────────────
    mcp_ctx = None
    verbai_token = os.environ.get("VERBAI_TOKEN", "")
    if verbai_token:
        tool_name, session_id = _init_mcp_session(VERBAI_MCP_URL, verbai_token)
        if tool_name:
            mcp_ctx = (tool_name, session_id, VERBAI_MCP_URL, verbai_token)
    else:
        print("[INFO] VERBAI_TOKEN not set — using Claude CLI only")

    # ── Fetch data sets ───────────────────────────────────────────────────────
    # fetch_category_counts removed: category engagement counts are now derived
    # directly from the comprehensive all_events list in build_political_data_from_events.
    if mcp_ctx:
        # Direct MCP: parallel — Snowflake cold-starts can take 60-90s each,
        # running sequentially stacks them (5 × 2 min = 10+ min wall time).
        print("[INFO] Running 5 direct MCP queries in parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            f_queries     = pool.submit(fetch_search_queries,      since_iso, mcp_ctx)
            f_youtube     = pool.submit(fetch_youtube_videos,      since_iso, mcp_ctx)
            f_news        = pool.submit(fetch_news_articles,       since_iso, mcp_ctx)
            f_tiktok_wtch = pool.submit(fetch_tiktok_watch_videos, since_iso, mcp_ctx)
            f_live        = pool.submit(fetch_live_events,         since_iso, mcp_ctx)
            queries_raw      = f_queries.result()
            youtube_raw      = f_youtube.result()
            news_raw         = f_news.result()
            tiktok_watch_raw = f_tiktok_wtch.result()
            live_events_raw  = f_live.result()
    else:
        # Claude fallback: subprocesses in parallel
        print("[INFO] Running 5 Claude queries in parallel (timeout=120s each)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            f_queries     = pool.submit(fetch_search_queries,      since_iso, None)
            f_youtube     = pool.submit(fetch_youtube_videos,      since_iso, None)
            f_news        = pool.submit(fetch_news_articles,       since_iso, None)
            f_tiktok_wtch = pool.submit(fetch_tiktok_watch_videos, since_iso, None)
            f_live        = pool.submit(fetch_live_events,         since_iso, None)
            queries_raw      = f_queries.result()
            youtube_raw      = f_youtube.result()
            news_raw         = f_news.result()
            tiktok_watch_raw = f_tiktok_wtch.result()
            live_events_raw  = f_live.result()

    # ── First-run-of-day: fetch new youth polling data ────────────────────────
    # Runs only during the 12:15 am ET cron (ET hour == 0, minute < 30) so we
    # don't hammer VerbAI with polling queries on every 15-minute tick.
    _now_utc   = datetime.datetime.now(datetime.timezone.utc)
    _et_off_h  = 4 if 4 <= _now_utc.month <= 10 else 5
    _now_et    = _now_utc - datetime.timedelta(hours=_et_off_h)
    _first_run = (_now_et.hour == 0 and _now_et.minute < 30)
    if _first_run:
        print("[INFO] First cron of the day — checking for new youth polling data...")
        _midnight_iso = _now_et.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        new_polls = fetch_new_polling(_midnight_iso, mcp_ctx)
        update_polling_data(new_polls)
    else:
        print(f"[INFO] Polling check skipped (ET time {_now_et.strftime('%H:%M')} — not first run).")

    # ── Build master event list (single source of truth for the 24h window) ────
    #
    # Step 1: Enrich individual search/reddit events from fetch_live_events with
    #         the aggregate counts returned by fetch_search_queries.  Queries that
    #         appear in queries_raw get their population-level count; novel or very
    #         recent searches not yet in the aggregate query keep count=1 — they
    #         still surface in the feed and the dashboard, just with lower weight.
    enriched_live = _enrich_events_with_aggregate_counts(
        live_events_raw or [], queries_raw or []
    )
    print(f"[INFO] Enriched {len(enriched_live)} live events with aggregate counts.")

    # Step 2: Convert YouTube / TikTok-watch / News aggregate items to live events
    #         so every source is represented in the same event stream.
    media_events = _raw_media_to_live_events(youtube_raw, tiktok_watch_raw, news_raw, since_iso)
    print(f"[INFO] Converted {len(media_events)} media items to live events "
          f"(yt={len(youtube_raw or [])}, tt={len(tiktok_watch_raw or [])}, "
          f"news={len(news_raw or [])}).")

    # Step 3: Merge into one comprehensive event list
    all_events = enriched_live + media_events

    any_data = bool(all_events)

    if not any_data:
        # Transient VerbAI outage — keep existing file, advance timestamp only.
        print("[WARN] VerbAI returned no data — keeping existing JSON unchanged.")
        if OUTPUT_FILE.exists():
            with open(OUTPUT_FILE) as f:
                existing = json.load(f)
            if existing.get("meta"):
                existing["meta"]["generated_at"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                existing["meta"].setdefault("last_mcp_pull", existing["meta"]["generated_at"])
                existing["meta"]["today_start"] = since_iso
                with open(OUTPUT_FILE, "w") as f:
                    json.dump(existing, f, indent=2, ensure_ascii=False)
                update_history(existing)
    else:
        # ── Determine whether to reset live_feed (ET day rollover) ────────────
        # Within the same ET calendar day: accumulate so events from earlier in
        # the day are not lost between cron runs.  On rollover: start fresh.
        _lf_reset = True
        if LIVE_FEED_FILE.exists():
            try:
                with open(LIVE_FEED_FILE) as _lf_f:
                    _lf_gen = json.load(_lf_f).get("generated_at", "")
                _lf_utc  = datetime.datetime.strptime(_lf_gen[:19], "%Y-%m-%dT%H:%M:%S")
                _et_off  = datetime.timedelta(hours=(4 if 4 <= datetime.datetime.now(datetime.timezone.utc).month <= 10 else 5))
                _now_et  = (datetime.datetime.now(datetime.timezone.utc) - _et_off).date()
                _lf_et   = (_lf_utc - _et_off).date()
                _lf_reset = (_lf_et != _now_et)
            except Exception:
                pass

        # ── 1. Write live_feed.json — the master 24h dataset ─────────────────
        # Contains every political engagement this window: individual search/reddit
        # events (count-enriched) + YouTube/TikTok/News items as event records.
        # Rolling eviction keeps only the last 24 hours; events that age out are
        # permanently preserved in sources_cache below.
        _append_live_events(all_events, reset=_lf_reset)

        # ── 2. Write sources_cache — permanent archive, never evicts ─────────
        # Every run appends this run's events so the Sources tab week/month views
        # always have the full history even after live_feed.json rolls forward.
        write_sources_cache_from_events(all_events)

        # ── 3. Derive political_data.json from the master event list ──────────
        # Dashboard, dial, word cloud, and category cards all read from here.
        # Building from all_events ensures they reflect the same complete picture
        # as the live feed — no independent popularity-filtered path.
        data = build_political_data_from_events(all_events, since_iso)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"[OK] Wrote {OUTPUT_FILE.name} — "
              f"{data['summary']['total_engagements']} engagements across "
              f"{data['summary']['categories_tracked']} categories.")

        # ── 4. Sentiment history ───────────────────────────────────────────────
        update_history(data)

    # ── Write MCP status (every run, data or not) ─────────────────────────────
    write_mcp_status(
        data_returned=any_data,
        counts={
            "queries":      len(queries_raw      or []),
            "youtube":      len(youtube_raw       or []),
            "news":         len(news_raw          or []),
            "tiktok_watch": len(tiktok_watch_raw  or []),
            "live_events":  len(live_events_raw   or []),
        },
    )


if __name__ == "__main__":
    main()
