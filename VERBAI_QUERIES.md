# VerbAI Query Documentation

## Overview

This dashboard integrates with VerbAI MCP (Model Context Protocol) to fetch real-time political engagement data from users aged 18-29. The integration uses the Claude CLI to execute structured queries against VerbAI data sources (Google Search, News, Reddit).

**Data Sources:**
- Google Search Events (`SEARCH_EVENTS_FLAT_DYM`)
- Reddit Events (`REDDIT_EVENTS_FLAT_DYM`)
- News Articles

**Execution Method:**
All queries are executed via the Claude CLI with the VerbAI MCP enabled:
```bash
claude --no-interactive --output-format json --prompt "<PROMPT>" --mcp verb-ai-mcp
```

---

## Query 1: Category Engagement Counts

### Purpose
Fetch per-category engagement metrics and unique user counts for today's political topics.

### Query Details

**Function:** `fetch_category_counts()` ([fetch_data.py:68-104](fetch_data.py#L68))

**VerbAI Prompt:**
```
For users aged 18-29, what are the top political topics they are engaging with
across news, search, and Reddit TODAY ONLY — use event_time >= CURRENT_DATE to filter
so data starts from midnight of today and accumulates through now.
Group by policy category (Presidential Politics,
General Politics, Elections & Voting, Foreign Policy, Immigration Policy,
Legislative Politics, Economic Policy, Healthcare Policy, Education Policy,
Environmental Policy, Civil Rights). Return engagement_count and unique_users per category.
Respond only with a JSON array of objects with keys:
policy_category, engagement_count, unique_users.
```

### Response Structure

**Expected Return:** JSON array of category objects

```json
[
  {
    "policy_category": "Presidential Politics",
    "engagement_count": 150,
    "unique_users": 45
  },
  {
    "policy_category": "Economic Policy",
    "engagement_count": 120,
    "unique_users": 38
  }
  // ... more categories
]
```

### Response Field Definitions

| Field | Type | Description |
|-------|------|-------------|
| `policy_category` | string | Named policy category (e.g., "Presidential Politics") |
| `engagement_count` | integer | Total engagement events for this category today |
| `unique_users` | integer | Unique user count engaging with this category today |

### Processing

1. VerbAI groups search/news/Reddit events by policy category
2. Filters to users aged 18-29 only
3. Filters to events with `event_time >= CURRENT_DATE` (starts from midnight)
4. Returns aggregated counts per category
5. Falls back to existing `political_data.json` if query fails

### Refresh Interval
**5 minutes** (controlled by cron or scheduler)

---

## Query 2: Top Trending Items (Search & Reddit)

### Purpose
Fetch the top 40 trending search queries and Reddit posts from today, categorized by topic.

### Query Details

**Function:** `fetch_search_queries()` ([fetch_data.py:107-144](fetch_data.py#L107))

**VerbAI Prompt:**
```
For users aged 18-29, show the top 40 trending items from TODAY ONLY
(event_time >= CURRENT_DATE, meaning from midnight until now).
Combine: (1) top search queries from SEARCH_EVENTS_FLAT_DYM ordered by count,
and (2) top Reddit posts from REDDIT_EVENTS_FLAT_DYM ordered by score.
For each item include: the exact query or post title, count or score,
which source it came from (search or reddit), subreddit name if Reddit,
and which broad policy/topic category it belongs to
(e.g. Government & Accountability, Immigration & Civil Liberties,
Economic Inequality, Foreign Policy & World, Criminal Justice,
Corporate Power & Consumers, Culture & Media, Environment & Science,
Elections & Political Figures, Healthcare Policy, Education Policy).
Respond only with a JSON array of objects with keys:
query, topic, count, source, subreddit, category, trend (up/down/stable).
```

### Response Structure

**Expected Return:** JSON array of trending item objects

```json
[
  {
    "query": "climate crisis policy",
    "topic": "Climate Policy Debate",
    "count": 8342,
    "source": "search",
    "subreddit": null,
    "category": "Environment & Science",
    "trend": "up"
  },
  {
    "query": "Reddit giving DHS data on users",
    "topic": "DHS Data Privacy Concerns",
    "count": 34688,
    "source": "reddit",
    "subreddit": "law",
    "category": "Government & Accountability",
    "trend": "up"
  }
  // ... up to 40 items
]
```

### Response Field Definitions

| Field | Type | Description |
|-------|------|-------------|
| `query` | string | The exact search query text or Reddit post title |
| `topic` | string | Human-readable topic/title |
| `count` | integer | Engagement count (for search) or Reddit score |
| `source` | string | Data source: `"search"`, `"reddit"`, or `"news"` |
| `subreddit` | string \| null | Subreddit name (Reddit only, null for search) |
| `category` | string | Policy category (e.g., "Government & Accountability") |
| `trend` | string | Trend indicator: `"up"`, `"down"`, or `"stable"` |

### Processing

1. Queries both `SEARCH_EVENTS_FLAT_DYM` and `REDDIT_EVENTS_FLAT_DYM`
2. Limits to top 40 combined items
3. Filters to users aged 18-29 only
4. Filters to `event_time >= CURRENT_DATE`
5. Sorts by engagement (count/score)
6. Categorizes each item into a broad policy category
7. Determines trend direction (up/down/stable)
8. Falls back to existing data if query fails

### Data Normalization ([fetch_data.py:214-232](fetch_data.py#L214))

The raw response is normalized into dashboard format:
- **URL construction for Reddit items:** Generates searchable Reddit URLs based on subreddit and title
- **Count preservation:** Uses exact count/score from VerbAI
- **Trend mapping:** Preserves trend indicators for visualization

### Refresh Interval
**5 minutes for search queries** (controlled by cron or scheduler)

---

## Query 3: Live Events Feed

### Purpose
Fetch the 50 most recent individual political events (searches/posts) from today for real-time dashboard display.

### Query Details

**Function:** `fetch_live_events()` ([fetch_data.py:147-180](fetch_data.py#L147))

**VerbAI Prompt:**
```
Show the 50 most recent individual political events from VerbAI users aged 18-29
from TODAY ONLY (event_time >= CURRENT_DATE), ordered by event_time DESC.
Combine search events from SEARCH_EVENTS_FLAT_DYM and Reddit events from
REDDIT_EVENTS_FLAT_DYM. For each event include: event_time in ISO 8601 format,
the exact search query or Reddit post title, source (search or reddit),
subreddit name if Reddit (else null), and broad political category
(e.g. Presidential Politics, Immigration Policy, Economic Policy, Foreign Policy, etc.).
Respond only with a JSON array of objects with keys:
time, query, source, subreddit, category.
```

### Response Structure

**Expected Return:** JSON array of recent events, newest first

```json
[
  {
    "time": "2026-02-19T14:32:15Z",
    "query": "Ukraine military aid bill",
    "source": "search",
    "subreddit": null,
    "category": "Foreign Policy"
  },
  {
    "time": "2026-02-19T14:31:42Z",
    "query": "Student loan forgiveness update",
    "source": "search",
    "subreddit": null,
    "category": "Education Policy"
  },
  {
    "time": "2026-02-19T14:30:18Z",
    "query": "Tax cuts debate congress",
    "source": "reddit",
    "subreddit": "politics",
    "category": "Economic Policy"
  }
  // ... up to 50 events
]
```

### Response Field Definitions

| Field | Type | Description |
|-------|------|-------------|
| `time` | string | Event timestamp in ISO 8601 format (UTC) |
| `query` | string | The search query or Reddit post title |
| `source` | string | Data source: `"search"` or `"reddit"` |
| `subreddit` | string \| null | Subreddit name (Reddit only, null for search) |
| `category` | string | Political category |

### Processing

1. Queries both `SEARCH_EVENTS_FLAT_DYM` and `REDDIT_EVENTS_FLAT_DYM`
2. Limits to 50 most recent events
3. Filters to users aged 18-29 only
4. Filters to `event_time >= CURRENT_DATE`
5. Orders by `event_time DESC` (newest first)
6. Returns timestamp in ISO 8601 format
7. Falls back to existing `live_feed.json` if query fails

### Output Destination
Data is written to `live_feed.json` with wrapper:
```json
{
  "generated_at": "2026-02-19T14:35:00Z",
  "events": [ /* 50 events */ ]
}
```

### Refresh Interval
**2 minutes** (polling frequency in dashboard, data from VerbAI is updated continuously)

---

## Query Execution Flow

```
┌─────────────────────────────────────────┐
│   fetch_data.py main()                   │
│   (triggered every 5 minutes)            │
└──────────────┬──────────────────────────┘
               │
        ┌──────┴──────┬─────────────┬──────────────────┐
        │             │             │                  │
        ▼             ▼             ▼                  ▼
┌──────────────┐ ┌──────────────┐ ┌────────────────┐ ┌─────────────┐
│ fetch_       │ │ fetch_       │ │ fetch_live_    │ │ merge_into_ │
│ category_    │ │ search_      │ │ events()       │ │ structure() │
│ counts()     │ │ queries()    │ │                │ │             │
│              │ │              │ │ → live_feed    │ │ → writes    │
│ → [cats]     │ │ → [items]    │ │   .json        │ │ political_  │
└──────┬───────┘ └──────┬───────┘ └────────────────┘ │ data.json   │
       │                │                            └─────────────┘
       │                │
       └────────────────┘
              │
              ▼
    ┌──────────────────────────┐
    │  Dashboard Consumption   │
    │  (index.html)            │
    │  - Renders categories    │
    │  - Displays trends       │
    │  - Shows live feed       │
    └──────────────────────────┘
```

---

## Error Handling & Fallback Strategy

### Query Failure Scenarios

1. **Claude CLI returns non-zero exit code:**
   - Warning logged: `"[WARN] claude CLI returned {returncode}"`
   - Function returns `None`
   - Falls back to existing JSON file data

2. **Timeout (120 seconds):**
   - Warning logged: `"[WARN] VerbAI query failed: TimeoutExpired"`
   - Function returns `None`
   - Falls back to existing JSON file data

3. **JSON parsing error:**
   - Warning logged: `"[WARN] VerbAI query failed: JSONDecodeError"`
   - Function returns `None`
   - Falls back to existing JSON file data

4. **Claude CLI not installed:**
   - Warning logged: `"[WARN] VerbAI query failed: FileNotFoundError"`
   - Function returns `None`
   - Falls back to existing JSON file data

### Fallback Behavior

If all three queries fail:
- Existing `political_data.json` is preserved unchanged
- Dashboard continues showing previous data
- Console warns: `"[WARN] VerbAI returned no category/query data — keeping existing JSON unchanged"`

---

## Category Mapping

VerbAI returns dynamic category names that are mapped to dashboard categories:

| VerbAI Category | Dashboard ID | Dashboard Label | Icon |
|-----------------|--------------|-----------------|------|
| Various | `presidential_politics` | Presidential Politics | 🏛️ |
| Various | `general_politics` | General Politics | 🗳️ |
| Various | `elections_voting` | Elections & Voting | 🗳️ |
| Various | `foreign_policy` | Foreign Policy | 🌍 |
| Various | `immigration_policy` | Immigration Policy | 🛂 |
| Various | `legislative_politics` | Legislative Politics | 📜 |
| Various | `economic_policy` | Economic Policy | 💰 |
| Various | `healthcare_policy` | Healthcare Policy | 🏥 |
| Various | `education_policy` | Education Policy | 🎓 |
| Various | `environmental_policy` | Environmental Policy | 🌿 |
| Various | `civil_rights` | Civil Rights | ✊ |

---

## Data Pipeline Summary

| Step | Component | Input | Output | Frequency |
|------|-----------|-------|--------|-----------|
| 1 | VerbAI | Raw event stream | Aggregated data | Real-time |
| 2 | fetch_data.py | Three queries | political_data.json, live_feed.json | Every 5 min |
| 3 | index.html | JSON files | Dashboard UI | Every 5 min (search), 15 min (reddit) |
| 4 | Browser | Dashboard state | Live feed updates | Every 2 min (polling) |

---

## Integration Notes

### Claude CLI Requirements
- Claude CLI must be installed and configured
- VerbAI MCP (`verb-ai-mcp`) must be registered
- Claude API access required

### Current Limitations
- Queries timeout after 120 seconds
- Limited to top 40 trending items per query
- Limited to 50 most recent events per query
- No retry logic for failed queries (relies on 5-minute refresh cycle)
- Age filter hardcoded to 18-29 years old

### Future Enhancement Opportunities
- Configurable age ranges
- Exponential backoff retry on failures
- Query result caching
- Incremental event streaming instead of full refresh
- Real-time event filtering on dashboard client
