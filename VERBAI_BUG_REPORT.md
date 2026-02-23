# VerbAI Bug Report — 2026-02-20 through 2026-02-23

**Last updated:** 2026-02-23
**Dashboard:** SOTU Gen Z Political Engagement Dashboard
**MCP Endpoint:** `https://zknnynm-exc60781.snowflakecomputing.com/api/v2/databases/KAFKA_DATA/schemas/DOORDASH_EVENTS/mcp-servers/VERB_AI_MCP_SERVER`
**Schema reference:** https://docs.generationlab.org/getting-started/editor

---

## Schema Confirmation

Our queries use the table and column names exactly as documented in the VerbAI schema reference. The following fields were verified against the published schema before this run:

| Table | Columns used | Schema status |
|---|---|---|
| `AGENT_SYNC` | `USER_ID`, `YEAR_OF_BIRTH`, `GENDER`, `FULL_ADDRESS` | ✅ All documented |
| `SEARCH_EVENTS_FLAT_DYM` | `USER_ID`, `QUERY`, `EVENT_TIME` | ✅ All documented |
| `REDDIT_EVENTS_FLAT_DYM` | `USER_ID`, `TITLE`, `SUBREDDIT`, `SCORE`, `EVENT_TIME` | ✅ All documented |
| `YOUTUBE_EVENTS_FLAT_DYM` | `USER_ID`, `VIDEO_TITLE`, `CHANNEL`, `EVENT_TIME` | ✅ Added Feb 20; returned data that run |

The SQL was written to the spec. The issues described below are with execution, not schema.

---

## What We Requested

We called the VerbAI agent tool via `tools/call` with a prompt asking it to run **two SQL queries** — one against `SEARCH_EVENTS_FLAT_DYM` and one against `REDDIT_EVENTS_FLAT_DYM` — and return combined results as a single JSON array.

**Search query (QUERY 1):**
```sql
SELECT s.QUERY, COUNT(*) AS count
FROM SEARCH_EVENTS_FLAT_DYM s
JOIN AGENT_SYNC a ON s.USER_ID = a.USER_ID
WHERE s.EVENT_TIME >= '2026-02-20T05:00:00Z'
  AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29
  AND (
    s.QUERY ILIKE '%trump%' OR s.QUERY ILIKE '%congress%'
    OR s.QUERY ILIKE '%election%' OR s.QUERY ILIKE '%immigration%'
    -- ... ~38 political keyword conditions total
  )
GROUP BY s.QUERY ORDER BY count DESC LIMIT 20;
```

**Reddit query (QUERY 2):**
```sql
SELECT r.TITLE AS query, COUNT(*) AS count, r.SUBREDDIT
FROM REDDIT_EVENTS_FLAT_DYM r
JOIN AGENT_SYNC a ON r.USER_ID = a.USER_ID
WHERE r.EVENT_TIME >= '2026-02-20T05:00:00Z'
  AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29
  AND (
    LOWER(r.SUBREDDIT) IN (
      'politics','politicaldiscussion','conservative','liberal',
      'worldnews','news','neutralpolitics','geopolitics','economics',
      'economy','environment','climate','healthcare','immigration',
      'supremecourt','law','progressive','democrats','republican',
      'political_humor','libertarian','uspolitics','americanpolitics'
    )
    OR r.TITLE ILIKE '%trump%' OR r.TITLE ILIKE '%congress%'
    OR r.TITLE ILIKE '%immigration%' OR r.TITLE ILIKE '%ukraine%'
    -- ... 12 more political title keyword conditions
  )
GROUP BY r.TITLE, r.SUBREDDIT ORDER BY count DESC LIMIT 20;
```

**Expected:** `count` = number of panel event rows per item (`COUNT(*)`), filtered to political keywords (search) or political subreddits/titles (Reddit), by users aged 18–29.

---

## What Was Returned

Four distinct problems were observed in the response:

### Problem 1 — Search `QUERY ILIKE` keyword filter not enforced

The search query explicitly filters to rows where `s.QUERY` matches at least one of ~38 political keyword conditions. The following items came back in the response and passed none of those conditions:

| query | count | Matches any political keyword? |
|---|---|---|
| `stranger things` | 15 | ❌ |
| `no hands (feat. roscoe dash & wale)` | 15 | ❌ |
| `City` | 23 | ❌ |

None of these match `%trump%`, `%congress%`, `%election%`, `%immigration%`, or any of the other ~38 ILIKE conditions. The `WHERE s.QUERY ILIKE ...` clause was not applied.

### Problem 2 — `count` values are Reddit `SCORE` (upvotes), not `COUNT(*)`

The documented `REDDIT_EVENTS_FLAT_DYM.SCORE` column holds the Reddit upvote score for each post. Our SQL asks for `COUNT(*) AS count` — the number of VerbAI panel event rows. The returned values match Reddit upvote scores, not panel engagement counts:

| query | returned `count` | plausible as panel `COUNT(*)`? | plausible as Reddit `SCORE`? |
|---|---|---|---|
| `came home and my cats feet are yellow?` | 23,789 | ❌ Impossible (23K events from a ~3K user panel) | ✅ Typical viral post score |
| `Former South Korean President Yoon Sentenced...` | 6,923 | ❌ Implausibly high | ✅ Consistent with r/worldnews upvotes |
| `Everybody Hates Nuclear-Chan` | 3,647 | ❌ Implausibly high | ✅ Consistent with r/comics upvotes |

The agent appears to be returning `SCORE` in place of the `COUNT(*)` aggregate.

### Problem 3 — `SUBREDDIT IN (...)` whitelist not enforced

9 of ~17 Reddit items came from subreddits absent from the whitelist and with no political keywords in their titles:

| subreddit | count | query | In whitelist? |
|---|---|---|---|
| `r/cats` | 23,789 | `came home and my cats feet are yellow?` | ❌ |
| `r/memes` | 12,426 | `They were real Chads` | ❌ |
| `r/pcmasterrace` | 12,184 | `discord right now:` | ❌ |
| `r/losercity` | 4,377 | `Kitty's first date` | ❌ |
| `r/losercity` | 3,744 | `Losercity Quake memes` | ❌ |
| `r/losercity` | 3,619 | `Losercity gluten` | ❌ |
| `r/nbacirclejerk` | 1,557 | `Barely into June and she's already acting out` | ❌ |
| `r/worldnews` | 6,923 | `Former South Korean President Yoon Sentenced to Life in Prison` | ✅ |
| `r/interestingasfuck` | 9,455 | `Bro went to space just to never return to his "country"` | ❌ |

All non-whitelisted items were categorised as `"General Politics"` by the agent. The `LOWER(r.SUBREDDIT) IN (...)` WHERE clause was not applied before results were returned.

### Problem 4 — Inconsistent results across two calls in the same session

A second call in the same workflow run (`fetch_category_counts`) used equivalent SQL asking for aggregate engagement grouped by `policy_category`. That call returned **0 rows**, while this call returned ~70 rows. Both used the same MCP session, the same Snowflake tables, and the same time window and age filter. The SQL does not appear to be executed deterministically across calls.

---

## Note on Age Range

The published schema states the `AGENT_SYNC` panel covers ages **21–34**. Our SQL filters `BETWEEN 18 AND 29`. In practice this means the cohort being queried is **ages 21–29** — there are no 18–20 year old panelists. This is not a blocking bug but worth flagging: the dashboard labels the data "Ages 18-29" and we would update that label to "Ages 21-29" if this is confirmed as a hard panel constraint.

---

## Full Output

The complete broken output is preserved in `political_data.json` at git commit `a0dd54f`.

---

## Update — 2026-02-23: Complete Data Blackout (Feb 21–23)

Problem 4 (non-deterministic results) has since escalated into a **total blackout**. Every workflow run from Feb 20 16:33 UTC through Feb 23 has returned **zero rows from all three fetch calls** simultaneously:

| Fetch call | Feb 20 last run | Feb 21 | Feb 22 | Feb 23 |
|---|---|---|---|---|
| `fetch_category_counts` | 0 rows (already broken) | 0 rows | 0 rows | 0 rows |
| `fetch_search_queries` | data returned | 0 rows | 0 rows | 0 rows |
| `fetch_youtube_videos` | 10 items returned | 0 rows | 0 rows | 0 rows |

The workflow has been running successfully (token present, MCP session established, `tools/call` completes without error) — VerbAI is just returning empty content blocks on every call.

### Downstream impact

The 3-day blackout caused the following visible failures in the dashboard:

- **History graph gap** — `history.json` has no points for Feb 21, 22, or 23. The graph shows a flat line ending Feb 20 16:33 and then goes blank. (Workaround committed: the no-data path now appends a history point on every run so gaps don't accumulate further.)
- **Stale date label** — The dashboard showed "Today (Feb 20) · Updated live" on Feb 21, 22, and 23 because `today_start` and `window_label` were not refreshed in the no-data path. (Workaround committed.)
- **Stale engagement counts** — All category totals (42,698 engagements) and individual items frozen at Feb 20 values. The five YouTube videos from Feb 20 continue to dominate the display.
- **Stale live feed** — `live_feed.json` last written Feb 20 03:59 UTC. Live events panel shows Feb 19 queries as "live."

### State of Feb 20 data now on the dashboard

Given Problems 1–3 from the original report, the Feb 20 data currently displayed is also of questionable accuracy:
- The 42,698 "total engagements" figure is dominated by five YouTube items whose counts appear to reflect video view/like counts rather than panel `COUNT(*)` values (e.g. 9,870 for one video is implausibly high for a ~3K-user panel).
- The data pipeline returned these inflated counts on the one run that succeeded; subsequent runs have returned nothing.

---

## Requested Fix

1. **Search keyword filter** — Apply the `s.QUERY ILIKE '%...'` conditions as written. Only search rows matching at least one political keyword should appear in results.
2. **`COUNT(*)`** — Execute the aggregate as written. The `count` field should reflect the number of VerbAI panel event rows matching the WHERE clause, not the post's Reddit `SCORE`.
3. **Subreddit filter** — Apply the `LOWER(r.SUBREDDIT) IN (...)` WHERE clause before returning results. Only rows matching the whitelist (or a title keyword condition) should appear.
4. **Determinism** — Two calls with equivalent SQL in the same session should return consistent non-zero results when data exists in the time window.
5. **Complete blackout (new, blocking)** — All three fetch calls have returned zero rows on every run since Feb 20 16:33 UTC. The MCP session establishes successfully and `tools/call` completes without a JSON-RPC error, but content blocks are empty. This is the highest-priority issue: the dashboard has been fully stale for 3+ days. We need either (a) confirmation that the Snowflake tables have data in the Feb 21–23 window and a fix for why queries return empty, or (b) a status update if there is a known data pipeline outage on VerbAI's end.

## Note on YouTube (`YOUTUBE_EVENTS_FLAT_DYM`)

We added YouTube queries (via `JOIN AGENT_SYNC`) on Feb 20 and received 10 results in that run, confirming the table is populated and the join pattern works. However, YouTube fetch calls have also returned zero rows since Feb 20, consistent with the broader blackout described above. Once the blackout is resolved we expect YouTube data to resume normally.
