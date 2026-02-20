# VerbAI Bug Report — 2026-02-20

**Date:** 2026-02-20
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

The SQL was written to the spec. The issues described below are with execution, not schema.

---

## What We Requested

We called the VerbAI agent tool via `tools/call` with a prompt containing explicit SQL. The Reddit query:

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

**Expected:** `count` = number of panel event rows per post (`COUNT(*)`), from political subreddits or posts with political keywords, by users aged 18–29, ordered by panel engagement.

---

## What Was Returned

Three distinct problems were observed in the response:

### Problem 1 — `count` values are Reddit `SCORE` (upvotes), not `COUNT(*)`

The documented `REDDIT_EVENTS_FLAT_DYM.SCORE` column holds the Reddit upvote score for each post. Our SQL asks for `COUNT(*) AS count` — the number of VerbAI panel event rows. The returned values match Reddit upvote scores, not panel engagement counts:

| query | returned `count` | plausible as panel `COUNT(*)`? | plausible as Reddit `SCORE`? |
|---|---|---|---|
| `came home and my cats feet are yellow?` | 23,789 | ❌ Impossible (23K events from a ~3K user panel) | ✅ Typical viral post score |
| `Former South Korean President Yoon Sentenced...` | 6,923 | ❌ Implausibly high | ✅ Consistent with r/worldnews upvotes |
| `Everybody Hates Nuclear-Chan` | 3,647 | ❌ Implausibly high | ✅ Consistent with r/comics upvotes |

The agent appears to be returning `SCORE` in place of the `COUNT(*)` aggregate.

### Problem 2 — `SUBREDDIT IN (...)` whitelist not enforced

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

### Problem 3 — Inconsistent results across two calls in the same session

A second call in the same workflow run (`fetch_category_counts`) used equivalent SQL asking for aggregate engagement grouped by `policy_category`. That call returned **0 rows**, while this call returned ~70 rows. Both used the same MCP session, the same Snowflake tables, and the same time window and age filter. The SQL does not appear to be executed deterministically across calls.

---

## Note on Age Range

The published schema states the `AGENT_SYNC` panel covers ages **21–34**. Our SQL filters `BETWEEN 18 AND 29`. In practice this means the cohort being queried is **ages 21–29** — there are no 18–20 year old panelists. This is not a blocking bug but worth flagging: the dashboard labels the data "Ages 18-29" and we would update that label to "Ages 21-29" if this is confirmed as a hard panel constraint.

---

## Full Output

The complete broken output is preserved in `political_data.json` at git commit `a0dd54f`.

---

## Requested Fix

1. **`COUNT(*)`** — Execute the aggregate as written. The `count` field in the response should reflect the number of VerbAI panel event rows matching the WHERE clause, not the post's Reddit `SCORE`.
2. **Subreddit filter** — Apply the `LOWER(r.SUBREDDIT) IN (...)` WHERE clause before returning results. Only rows matching the whitelist (or a title keyword condition) should appear in the response.
3. **Determinism** — Two calls with equivalent SQL in the same session should return consistent non-zero results when data exists in the time window.
