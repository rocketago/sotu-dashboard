#!/usr/bin/env python3
"""
backfill_history.py — populate history.json with real VerbAI sentiment scores
for each day from Feb 15, 2026 through yesterday (today's data comes from the
live fetch_data.py run).

For each day it queries VerbAI for that day's top political searches, YouTube
videos and Reddit posts, then applies the same AFINN scoring used by the
dashboard to produce a real sentiment score.  Two data points are written per
day (morning ~9 AM ET and evening ~9 PM ET) so the chart has meaningful shape.

Usage:
    python3 backfill_history.py
"""

import datetime
import json
import re
import subprocess
from pathlib import Path

HISTORY_FILE = Path(__file__).parent / "history.json"

# ── AFINN lexicon (mirrors fetch_data.py + index.html) ───────────────────────
_AFINN: dict[str, int] = {
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
    "victory":4,"victories":4,"win":4,"wins":4,"winning":3,
    "freedom":4,"liberty":4,
    "justice":4,"peace":5,"ceasefire":4,
    "historic":3,"landmark":3,"breakthrough":4,
    "record":3,"save":3,"saved":3,"rights":2,
}

_CLEAN_RE = re.compile(r"[^a-z\s]")

_TRUMP_REPUB_KEYWORDS = (
    "trump", "republican", "republicans", "gop", "maga",
    "white house", "executive order", "conservative", "conservatives",
    "ivanka", "melania", "jd vance", "vance", "desantis", "rubio",
    "mcconnell", "haley", "pence",
)
_DEMOCRAT_LIB_KEYWORDS = (
    "democrat", "democrats", "democratic", "dems",
    "liberal", "liberals", "progressive", "progressives",
    "biden", "harris", "pelosi", "schumer", "aoc", "ocasio-cortez",
    "bernie", "sanders", "warren", "defund", "woke",
)


def score_item(topic: str, query: str) -> float:
    text = _CLEAN_RE.sub(" ", ((topic or "") + " " + (query or "")).lower())
    raw = sum(_AFINN.get(w, 0) for w in text.split())
    return max(0.0, min(100.0, raw * 5 + 50))


def score_item_sentiment(item: dict) -> float:
    """Return 0-100 sentiment score framed relative to Trump/Republicans.

    Democrat/liberal content (not also Republican) has its score inverted so
    that negative-about-Democrats counts as positive for Republicans.
    """
    topic = item.get("topic", "")
    query = item.get("query", "")
    text  = (topic + " " + query).lower()
    raw_score = score_item(topic, query)
    is_repub = any(kw in text for kw in _TRUMP_REPUB_KEYWORDS)
    is_dem   = any(kw in text for kw in _DEMOCRAT_LIB_KEYWORDS)
    if is_dem and not is_repub:
        return 100.0 - raw_score
    return raw_score


def afinn_score_from_items(items: list[dict]) -> int | None:
    """Compute engagement-weighted sentiment score from a list of items."""
    if not items:
        return None
    total_w = sum(i.get("count", 1) for i in items) or 1
    total_s = sum(score_item_sentiment(i) * i.get("count", 1) for i in items)
    return round(total_s / total_w)


# ── SOTU-focused SQL helpers (mirrors fetch_data.py) ─────────────────────────
_SQL_INCLUDE_BLOCK = (
    "(col ILIKE '%trump%' OR col ILIKE '%white house%' OR col ILIKE '%maga%' "
    "OR col ILIKE '%executive order%' "
    "OR col ILIKE '%congress%' OR col ILIKE '%senate%' OR col ILIKE '%democrat%' "
    "OR col ILIKE '%republican%' OR col ILIKE '%legislation%' "
    "OR col ILIKE '%doge%' OR col ILIKE '%elon musk%' "
    "OR col ILIKE '%federal worker%' OR col ILIKE '%federal employee%' "
    "OR col ILIKE '%federal budget%' OR col ILIKE '%spending cut%' "
    "OR col ILIKE '%tariff%' OR col ILIKE '%trade war%' "
    "OR col ILIKE '%inflation%' OR col ILIKE '%unemployment%' "
    "OR col ILIKE '%immigration%' OR col ILIKE '%border%' OR col ILIKE '%deportation%' "
    "OR col ILIKE '%ice raid%' OR col ILIKE '%daca%' OR col ILIKE '%migrant%' "
    "OR col ILIKE '%ukraine%' OR col ILIKE '%russia%' OR col ILIKE '%nato%' "
    "OR col ILIKE '%china%' OR col ILIKE '%iran%' OR col ILIKE '%israel%' "
    "OR col ILIKE '%gaza%' OR col ILIKE '%north korea%' OR col ILIKE '%taiwan%' "
    "OR col ILIKE '%healthcare%' OR col ILIKE '%medicare%' OR col ILIKE '%medicaid%' "
    "OR col ILIKE '%obamacare%' OR col ILIKE '%abortion%' OR col ILIKE '%gun control%' "
    "OR col ILIKE '%supreme court%' OR col ILIKE '%social security%' "
    "OR col ILIKE '%student loan%' OR col ILIKE '%climate%' OR col ILIKE '%energy%' "
    "OR col ILIKE '%deficit%' OR col ILIKE '%debt ceiling%' OR col ILIKE '%budget%' "
    "OR col ILIKE '%state of the union%' OR col ILIKE '%sotu%' "
    "OR col ILIKE '%address to congress%' "
    "OR col ILIKE '%biden%' OR col ILIKE '%kamala%' OR col ILIKE '%rubio%' "
    "OR col ILIKE '%hegseth%' OR col ILIKE '%gabbard%') "
)
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


def _sql_include(col: str) -> str:
    return _SQL_INCLUDE_BLOCK.replace("col", col)

def _sql_exclude(col: str) -> str:
    return _SQL_EXCLUDE_BLOCK.replace("col", col)

def _sql_kw(col: str) -> str:
    return _sql_include(col) + _sql_exclude(col)


def run_claude_query(prompt: str, timeout: int = 300) -> str | None:
    """Run a prompt through the Claude CLI and return stdout."""
    import os
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    try:
        result = subprocess.run(
            ["claude", "--print", "--dangerously-skip-permissions", prompt],
            capture_output=True, text=True, timeout=timeout, env=env,
        )
        print(f"  [claude] exit={result.returncode} stdout={len(result.stdout)}b")
        if result.returncode != 0 or not result.stdout.strip():
            print(f"  [claude] stderr: {result.stderr[:300]}")
            return None
        return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        print(f"  [claude] error: {e}")
        return None


def parse_items(text: str) -> list[dict]:
    """Extract a JSON array of items from Claude's response."""
    if not text:
        return []
    DATA_KEYS = frozenset({"query", "count", "source", "topic", "category", "trend"})

    def is_data(arr):
        return isinstance(arr, list) and arr and isinstance(arr[0], dict) and DATA_KEYS.intersection(arr[0])

    def search(obj, depth=0):
        if depth > 10:
            return None
        if is_data(obj):
            return obj
        if isinstance(obj, list):
            for item in obj:
                r = search(item, depth + 1)
                if r:
                    return r
        elif isinstance(obj, dict):
            for key in ("result", "rows", "data", "items", "records", "output", "content"):
                if key in obj:
                    r = search(obj[key], depth + 1)
                    if r:
                        return r
            for v in obj.values():
                r = search(v, depth + 1)
                if r:
                    return r
        elif isinstance(obj, str) and len(obj) > 4 and obj.lstrip()[:1] in ("[", "{"):
            try:
                return search(json.loads(obj), depth + 1)
            except json.JSONDecodeError:
                pass
        return None

    try:
        result = search(json.loads(text))
        if result:
            return result
    except json.JSONDecodeError:
        pass
    try:
        m = re.search(r"\[.*\]", text, re.DOTALL)
        if m:
            arr = json.loads(m.group())
            if is_data(arr):
                return arr
    except (json.JSONDecodeError, AttributeError):
        pass
    return []


def fetch_day_items(start_iso: str, end_iso: str, label: str) -> list[dict]:
    """
    Query VerbAI for the top political content during [start_iso, end_iso).
    Uses a short natural-language prompt so the Claude CLI doesn't time out.
    """
    date_str = start_iso[:10]
    start_t  = start_iso[11:16]
    end_t    = end_iso[11:16]
    prompt = (
        f"Use the VerbAI Snowflake tool to find the top 30 political items consumed "
        f"by US adults aged 18-29 on {date_str} between {start_t} and {end_t} UTC. "
        f"Query SEARCH_EVENTS_FLAT_DYM, YOUTUBE_EVENTS_FLAT_DYM, and REDDIT_EVENTS_FLAT_DYM, "
        f"joining each with AGENT_SYNC on USER_ID for age filtering. "
        f"Focus ONLY on topics relevant to US politics and the 2026 State of the Union: "
        f"Trump, DOGE, tariffs, immigration/deportation, federal budget cuts, "
        f"Ukraine/Russia/NATO, Iran, China, healthcare, social security, climate/energy. "
        f"Exclude British royals, South Korean politics, celebrity gossip. "
        f"Return a raw JSON array (no markdown) where each item has: "
        f"query (title/search term), topic (one-line description), count (integer), "
        f"source ('search'/'youtube'/'reddit'), subreddit (null if not reddit), "
        f"channel (null if not youtube), trend ('up'/'down'/'stable')."
    )

    print(f"\n[{label}] Querying VerbAI for {start_iso[:10]} ({start_iso[11:16]}–{end_iso[11:16]} UTC)…")
    text = run_claude_query(prompt, timeout=480)
    items = parse_items(text)
    print(f"[{label}] Got {len(items)} items")
    for it in items[:5]:
        print(f"  count={it.get('count',0):5d} src={it.get('source','?'):7s} | {str(it.get('query',''))[:60]}")
    return items


def day_range_utc(date: datetime.date, start_hour: int, end_hour: int):
    """Return (start_iso, end_iso) for a UTC hour range on a given date."""
    tz = datetime.timezone.utc
    start = datetime.datetime(date.year, date.month, date.day, start_hour, 0, 0, tzinfo=tz)
    end   = datetime.datetime(date.year, date.month, date.day, end_hour,   0, 0, tzinfo=tz)
    fmt   = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt), end.strftime(fmt), start


def main():
    # Recompute sentiment for all days from Feb 15 through yesterday using the
    # new framing-aware scoring. Preserve only today's live data points.
    today      = datetime.date.today()
    start_date = datetime.date(2026, 2, 15)
    today_prefix = today.strftime("%Y-%m-%d")

    # Load existing history to preserve today's live data points
    existing_points: list[dict] = []
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE) as f:
            existing_points = json.load(f).get("points", [])
    # Keep only points from today (real live data) — we'll rebuild everything else
    real_points = [p for p in existing_points if p["ts"].startswith(today_prefix)]
    print(f"Preserving {len(real_points)} existing real data points (today: {today_prefix}).")

    new_points: list[dict] = []
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    tz  = datetime.timezone.utc

    # One query per day covering the full 24-hour window.
    # History point is placed at 17:00 UTC (~noon ET) for each day.
    date = start_date
    while date < today:
        start_dt = datetime.datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=tz)
        end_dt   = start_dt + datetime.timedelta(days=1)
        ts_dt    = datetime.datetime(date.year, date.month, date.day, 17, 0, 0, tzinfo=tz)

        start_iso = start_dt.strftime(fmt)
        end_iso   = end_dt.strftime(fmt)
        ts        = ts_dt.strftime(fmt)
        label     = date.strftime("%b %d")

        items = fetch_day_items(start_iso, end_iso, label)
        score = afinn_score_from_items(items)
        if score is None:
            print(f"  [SKIP] No data returned for {label} — skipping.")
        else:
            print(f"  → AFINN score for {label}: {score}")
            new_points.append({"ts": ts, "score": score})

        date += datetime.timedelta(days=1)

    # Merge: new backfill points + preserved real points, sorted by timestamp
    all_points = sorted(new_points + real_points, key=lambda p: p["ts"])

    print(f"\nTotal points: {len(all_points)} ({len(new_points)} new + {len(real_points)} preserved)")

    with open(HISTORY_FILE, "w") as f:
        json.dump({"points": all_points}, f, separators=(",", ":"))
    print(f"Written to {HISTORY_FILE}")

    # Show summary
    from collections import defaultdict
    by_day: dict[str, list[int]] = defaultdict(list)
    for p in all_points:
        by_day[p["ts"][:10]].append(p["score"])
    print("\nDaily summary:")
    for day, scores in sorted(by_day.items()):
        print(f"  {day}: avg={sum(scores)/len(scores):.1f}  points={scores}")


if __name__ == "__main__":
    main()
