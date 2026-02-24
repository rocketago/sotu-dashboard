# VerbAI Bug Report 3 — Persistent ~8-Hour Ingestion Lag Across All Event Tables

**Filed:** 2026-02-24
**Dashboard:** SOTU Gen Z Political Engagement Dashboard
**MCP Endpoint:** `https://zknnynm-exc60781.snowflakecomputing.com/api/v2/databases/KAFKA_DATA/schemas/DOORDASH_EVENTS/mcp-servers/VERB_AI_MCP_SERVER`

---

## Summary

As of 2026-02-24 at approximately 05:52 UTC, all three event tables (`SEARCH_EVENTS_FLAT_DYM`, `YOUTUBE_EVENTS_FLAT_DYM`, `REDDIT_EVENTS_FLAT_DYM`) are consistently **~8 hours behind wall-clock time**. This contradicts the expected real-time (or near-real-time) behavior described in the VerbAI documentation and makes the dashboard non-functional during early-morning hours when users are most interested in overnight and morning engagement data.

---

## Evidence

`MAX(EVENT_TIME)` queried directly against each table with no filters at **2026-02-24 05:52 UTC**:

| Table | `MAX(EVENT_TIME)` | Observed Lag | Total Rows |
|---|---|---|---|
| `SEARCH_EVENTS_FLAT_DYM` | `2026-02-23 21:35:17` | **~8h 17m** | 518,977 |
| `YOUTUBE_EVENTS_FLAT_DYM` | `2026-02-23 21:54:12` | **~7h 58m** | 6,043 (last 24h) |
| `REDDIT_EVENTS_FLAT_DYM` | `2026-02-23 21:17:07` | **~8h 35m** | 23,436 |

The lag is consistent across all three tables (within ~37 minutes of each other), suggesting a shared upstream pipeline delay rather than a table-specific issue.

---

## Impact

Our dashboard auto-update cron runs every 5 minutes and fetches data from midnight ET through the current time. At 12:52 AM ET (05:52 UTC) on Feb 24:

- VerbAI's latest data is from **9:17–9:54 PM ET on Feb 23**
- A query for `EVENT_TIME >= 2026-02-24T05:00:00Z` (midnight ET) returns **zero rows** from all tables
- The dashboard shows an empty state / stale data for the entire early-morning period
- Users who check the dashboard at night or in the early morning see no data at all

This is particularly impactful for a live political engagement dashboard where morning and overnight activity (e.g., breaking news responses, late-night social media activity) is significant.

---

## Prior Context

- **Bug Report 1** (filed ~2026-02-20): Documented query execution issues — WHERE clauses not enforced, `COUNT(*)` returning `SCORE` column values, non-deterministic results.
- **Bug Report 2** (filed 2026-02-23): Documented a complete data blackout from Feb 19–23. Data resumed as of ~Feb 23, confirming the pipeline restarted, but the ~8-hour lag has persisted since resumption.

---

## Questions for VerbAI Team

1. **Is the ~8-hour lag expected?** The product is described as real-time. Is there a documented SLA for ingestion latency that we missed?

2. **Is this a batch pipeline?** The consistent ~8-hour cutoff across all three tables suggests a batch job that runs once or twice a day rather than a streaming pipeline. If so, when does the batch run, and what is the expected freshness window?

3. **Is the lag constant or variable?** If the pipeline is truly streaming, is there a known backpressure or processing bottleneck causing the delay? Will it catch up during off-peak hours?

4. **What is `CURRENT_TIMESTAMP()` relative to in the Snowflake environment?** We verified our UTC math is correct. We want to confirm the Snowflake instance is not using a non-UTC timezone that could explain the apparent offset.

---

## Reproduction

```python
# Direct MCP call — run at any time and compare MAX_EVENT_TIME to wall clock
import json, urllib.request, datetime

# After MCP initialize + tools/list handshake, call tools/call with:
prompt = (
    "What is the maximum event_time in SEARCH_EVENTS_FLAT_DYM? "
    "What is the maximum event_time in YOUTUBE_EVENTS_FLAT_DYM? "
    "What is the maximum event_time in REDDIT_EVENTS_FLAT_DYM? "
)

# Expected (real-time): MAX_EVENT_TIME ≈ NOW() - seconds to minutes
# Observed: MAX_EVENT_TIME ≈ NOW() - 8 hours
```

To verify, compare the returned `MAX_EVENT_TIME` values to `datetime.datetime.utcnow()`. The delta should be under 5 minutes for a real-time pipeline; we consistently observe 8–8.5 hours.

---

## Workaround Applied

Until the lag is resolved, we changed our fetch logic to always query data from midnight ET (rather than using an incremental cursor based on the last successful pull). This avoids the cursor getting stuck ahead of VerbAI's data, but it does not solve the underlying empty-data problem during the ~8-hour overnight gap.
