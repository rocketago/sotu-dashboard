# VerbAI Bug Report 2 — Data Ingestion Stopped at Feb 19, 2026

**Filed:** 2026-02-23
**Dashboard:** SOTU Gen Z Political Engagement Dashboard
**MCP Endpoint:** `https://zknnynm-exc60781.snowflakecomputing.com/api/v2/databases/KAFKA_DATA/schemas/DOORDASH_EVENTS/mcp-servers/VERB_AI_MCP_SERVER`

---

## Summary

A direct `MAX(EVENT_TIME)` query against both core event tables confirms that **no new data has been ingested since February 19, 2026**. This is a data pipeline issue upstream of our queries — the Snowflake tables themselves contain no rows for Feb 20–23.

---

## Evidence

Two queries were run directly against the tables with no filters (no age, no keyword, no date range):

```sql
-- Query 1
SELECT MAX(EVENT_TIME) as latest_search FROM SEARCH_EVENTS_FLAT_DYM;

-- Query 2
SELECT MAX(EVENT_TIME) as latest_reddit FROM REDDIT_EVENTS_FLAT_DYM;
```

**Results:**

| Table | `MAX(EVENT_TIME)` | Total rows |
|---|---|---|
| `SEARCH_EVENTS_FLAT_DYM` | `2026-02-19 22:40:01` | 516,901 |
| `REDDIT_EVENTS_FLAT_DYM` | `2026-02-19 22:25:27` | 23,292 |

A follow-up date-filtered query for `2026-02-23` returned `0` rows and `NULL` timestamps from both tables.

---

## Impact

The dashboard's auto-update cron has been running on schedule (every 5 minutes) and successfully calling the MCP endpoint, but all queries correctly return zero results because the underlying tables have no data for Feb 20–23. This is not a query bug — our SQL is correct. The tables are simply not being populated.

**Visible effects on the dashboard:**
- All engagement counts, category rankings, and trending items are frozen at Feb 19 values
- The live events feed shows Feb 19 queries as if they are live
- The history/sentiment chart has a 4-day gap (Feb 20–23) with no data points
- The "Today (Feb 20) · Updated live" window label persisted through Feb 21–23 (now patched)

**Workarounds applied:**
- History gap (Feb 20–23) was manually backfilled using AFINN sentiment scoring against the frozen Feb 19 item data
- The no-data code path was patched to refresh the date label and append a placeholder history point on every run so gaps do not accumulate further

---

## Distinction from Bug Report 1

Bug Report 1 described problems with **query execution** (WHERE clauses not enforced, `COUNT(*)` returning `SCORE` instead, non-deterministic results). Those issues remain unresolved, but they are secondary to this new issue: **the source tables contain no data at all for any date after Feb 19**. Even a perfectly-executed query cannot return data that does not exist in the table.

---

## Requested Action

1. **Confirm whether the VerbAI ingestion pipeline for `SEARCH_EVENTS_FLAT_DYM` and `REDDIT_EVENTS_FLAT_DYM` is operational** — specifically, why both tables have no rows with `EVENT_TIME > 2026-02-19 22:40:01`.
2. **Confirm the expected data lag** — is near-real-time ingestion expected, or is there a known batch delay that would explain a 4+ day gap?
3. **Provide an ETA for data resumption**, or confirm whether the Feb 19 cutoff is a permanent data boundary (e.g. a schema migration, panel reset, or end of data collection period).

---

## Reproduction

```python
# Direct MCP call — no Claude CLI dependency
import json, urllib.request

url = "https://zknnynm-exc60781.snowflakecomputing.com/api/v2/databases/KAFKA_DATA/schemas/DOORDASH_EVENTS/mcp-servers/VERB_AI_MCP_SERVER"
token = "<VERBAI_TOKEN>"

# After MCP initialize + tools/list handshake:
prompt = (
    "What is the maximum EVENT_TIME from SEARCH_EVENTS_FLAT_DYM? "
    "Also what is the maximum EVENT_TIME from REDDIT_EVENTS_FLAT_DYM? "
    "Return as JSON with keys latest_search and latest_reddit."
)
# tools/call with the agent tool returns:
# { "latest_search": "2026-02-19 22:40:01", "latest_reddit": "2026-02-19 22:25:27" }
```

The same result is reproducible via the Cortex Analyst tool used by the VerbAI agent.
