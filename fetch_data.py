#!/usr/bin/env python3
"""
fetch_data.py — VerbAI political data refresher
Queries VerbAI MCP for Gen Z (18-29) political engagement data,
writes political_data.json for the dashboard to consume.

Run manually:
    python3 fetch_data.py

Or schedule with cron every 5 minutes:
    */5 * * * * cd /path/to/dashboard && python3 fetch_data.py >> fetch.log 2>&1
"""

import json
import subprocess
import sys
import datetime
import re
import random
from pathlib import Path

OUTPUT_FILE    = Path(__file__).parent / "political_data.json"
LIVE_FEED_FILE = Path(__file__).parent / "live_feed.json"

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


def run_verb_ai_query(prompt: str) -> dict | None:
    """
    Call the VerbAI MCP tool via the Claude CLI and return parsed JSON.
    Requires 'claude' CLI to be installed and configured with the VerbAI MCP.
    """
    cmd = [
        "claude",
        "--no-interactive",
        "--output-format", "json",
        "--prompt", prompt,
        "--mcp", "verb-ai-mcp",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            print(f"[WARN] claude CLI returned {result.returncode}: {result.stderr[:200]}")
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
        print(f"[WARN] VerbAI query failed: {e}")
        return None


def fetch_category_counts() -> list[dict]:
    """
    Query VerbAI for per-category engagement counts for 18-29 year-olds,
    starting from the beginning of today (CURRENT_DATE / midnight).
    Returns list of dicts with keys: label, engagement_count, unique_users.
    Falls back to the baked-in data on failure.
    """
    prompt = (
        "For users aged 18-29, what are the top political topics they are engaging with "
        "across news, search, and Reddit TODAY ONLY — use event_time >= CURRENT_DATE to filter "
        "so data starts from midnight of today and accumulates through now. "
        "Group by policy category (Presidential Politics, "
        "General Politics, Elections & Voting, Foreign Policy, Immigration Policy, "
        "Legislative Politics, Economic Policy, Healthcare Policy, Education Policy, "
        "Environmental Policy, Civil Rights). Return engagement_count and unique_users per category. "
        "Respond only with a JSON array of objects with keys: "
        "policy_category, engagement_count, unique_users."
    )
    raw = run_verb_ai_query(prompt)
    if not raw:
        return []

    # Parse from Claude's structured response
    try:
        if isinstance(raw, list):
            return raw
        # If wrapped in a message object, extract content
        content = raw.get("content", raw)
        if isinstance(content, str):
            match = re.search(r"\[.*\]", content, re.DOTALL)
            if match:
                return json.loads(match.group())
        if isinstance(content, list):
            return content
    except (json.JSONDecodeError, AttributeError):
        pass
    return []


def fetch_search_queries() -> list[dict]:
    """
    Query VerbAI for specific search queries AND top Reddit posts by 18-29 year-olds,
    starting from the beginning of today (event_time >= CURRENT_DATE).
    Returns list of dicts with keys: query, count, category, source, subreddit, trend.
    """
    prompt = (
        "For users aged 18-29, show the top 40 trending items from TODAY ONLY "
        "(event_time >= CURRENT_DATE, meaning from midnight until now). "
        "Combine: (1) top search queries from SEARCH_EVENTS_FLAT_DYM ordered by count, "
        "and (2) top Reddit posts from REDDIT_EVENTS_FLAT_DYM ordered by score. "
        "For each item include: the exact query or post title, count or score, "
        "which source it came from (search or reddit), subreddit name if Reddit, "
        "and which broad policy/topic category it belongs to "
        "(e.g. Government & Accountability, Immigration & Civil Liberties, "
        "Economic Inequality, Foreign Policy & World, Criminal Justice, "
        "Corporate Power & Consumers, Culture & Media, Environment & Science, "
        "Elections & Political Figures, Healthcare Policy, Education Policy). "
        "Respond only with a JSON array of objects with keys: "
        "query, topic, count, source, subreddit, category, trend (up/down/stable)."
    )
    raw = run_verb_ai_query(prompt)
    if not raw:
        return []

    try:
        if isinstance(raw, list):
            return raw
        content = raw.get("content", raw)
        if isinstance(content, str):
            match = re.search(r"\[.*\]", content, re.DOTALL)
            if match:
                return json.loads(match.group())
        if isinstance(content, list):
            return content
    except (json.JSONDecodeError, AttributeError):
        pass
    return []


def fetch_live_events() -> list[dict]:
    """
    Query VerbAI for the 50 most recent individual search/reddit events by 18-29
    year-olds today, ordered by recency. Powers the dashboard live feed panel.
    Returns list of dicts with keys: time, query, source, subreddit, category.
    """
    prompt = (
        "Show the 50 most recent individual political events from VerbAI users aged 18-29 "
        "from TODAY ONLY (event_time >= CURRENT_DATE), ordered by event_time DESC. "
        "Combine search events from SEARCH_EVENTS_FLAT_DYM and Reddit events from "
        "REDDIT_EVENTS_FLAT_DYM. For each event include: event_time in ISO 8601 format, "
        "the exact search query or Reddit post title, source (search or reddit), "
        "subreddit name if Reddit (else null), and broad political category "
        "(e.g. Presidential Politics, Immigration Policy, Economic Policy, Foreign Policy, etc.). "
        "Respond only with a JSON array of objects with keys: "
        "time, query, source, subreddit, category."
    )
    raw = run_verb_ai_query(prompt)
    if not raw:
        return []

    try:
        if isinstance(raw, list):
            return raw
        content = raw.get("content", raw)
        if isinstance(content, str):
            match = re.search(r"\[.*\]", content, re.DOTALL)
            if match:
                return json.loads(match.group())
        if isinstance(content, list):
            return content
    except (json.JSONDecodeError, AttributeError):
        pass
    return []


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
    total_users_set = set()

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
    max_eng = max((c["engagement_count"] for c in categories), default=1)
    for c in categories:
        c["trending_score"] = round((c["engagement_count"] / max_eng) * 100)

    top_cat  = categories[0]["label"] if categories else "N/A"
    now_utc  = datetime.datetime.utcnow()
    now_iso  = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    today    = datetime.date.today().isoformat()
    today_label = datetime.date.today().strftime("%b %-d")

    # Count today's events by source from items
    search_today = sum(i["count"] for c in categories for i in c["items"] if i.get("source") == "search")
    reddit_today = sum(1 for c in categories for i in c["items"] if i.get("source") == "reddit")

    return {
        "meta": {
            "generated_at": now_iso,
            "demographic":  "Ages 18-29",
            "data_source":  "VerbAI MCP (Search, News, Reddit events)",
            "window":       "today",
            "window_label": f"Today ({today_label}) · Updated live",
            "today_start":  f"{today}T00:00:00Z",
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


def seed_events_from_categories(cat_data: dict) -> list[dict]:
    """
    Synthesize live feed events from political_data.json category items.
    Used as a fallback when VerbAI returns no live events and live_feed.json
    is empty. Spreads items across today's time window (midnight → now).
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
        ev["time"] = (today_s + datetime.timedelta(seconds=delta)).strftime("%Y-%m-%dT%H:%M:%SZ")

    events.sort(key=lambda e: e["time"], reverse=True)
    return events


def main():
    print(f"[{datetime.datetime.now():%H:%M:%S}] Fetching VerbAI data...")

    categories_raw = fetch_category_counts()
    queries_raw    = fetch_search_queries()
    events_raw     = fetch_live_events()

    if not categories_raw and not queries_raw:
        print("[WARN] VerbAI returned no category/query data — keeping existing JSON unchanged.")
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
