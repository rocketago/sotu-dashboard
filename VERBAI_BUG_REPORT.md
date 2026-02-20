# VerbAI Bug Report — 2026-02-20

**Date:** 2026-02-20
**Dashboard:** SOTU Gen Z Political Engagement Dashboard
**MCP Endpoint:** `https://zknnynm-exc60781.snowflakecomputing.com/api/v2/databases/KAFKA_DATA/schemas/DOORDASH_EVENTS/mcp-servers/VERB_AI_MCP_SERVER`

---

## What We Requested

We called the VerbAI agent tool via `tools/call` with a prompt asking it to run two SQL queries and return combined results as a JSON array. The Reddit query included an explicit subreddit whitelist in the WHERE clause:

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

**Expected output:** top Reddit posts from political subreddits (or with political titles), engaged with by 18–29 year-old VerbAI panel users today.

---

## What Was Returned

The agent returned results that did not honour the `SUBREDDIT IN (...)` whitelist. 9 of ~17 Reddit items came from subreddits absent from the whitelist and with no political keywords in their titles:

| subreddit | count | query | Political? |
|---|---|---|---|
| `r/cats` | 23,789 | `came home and my cats feet are yellow?` | ❌ |
| `r/memes` | 12,426 | `They were real Chads` | ❌ |
| `r/pcmasterrace` | 12,184 | `discord right now:` | ❌ |
| `r/losercity` | 4,377 | `Kitty's first date` | ❌ |
| `r/losercity` | 3,744 | `Losercity Quake memes` | ❌ |
| `r/losercity` | 3,619 | `Losercity gluten` | ❌ |
| `r/nbacirclejerk` | 1,557 | `Barely into June and she's already acting out` | ❌ |
| `r/worldnews` | 6,923 | `Former South Korean President Yoon Sentenced to Life in Prison` | ✅ |
| `r/interestingasfuck` | 9,455 | `Bro went to space just to never return to his "country"` | ⚠️ |

All non-political items were categorised as `"General Politics"` by the agent.

The full output is preserved in `political_data.json` at git commit `a0dd54f`.

---

## Additional Finding — Inconsistent Results Across Two Calls in the Same Session

In the same workflow run, a second call (`fetch_category_counts`) asked for aggregate engagement counts grouped by `policy_category` using equivalent SQL. That call returned **0 rows**, while this call returned ~70 rows. Both used the same MCP session against the same Snowflake tables with the same time window and age filter. The inconsistency suggests the SQL is not being executed deterministically.

---

## Summary

The `LOWER(r.SUBREDDIT) IN (...)` WHERE clause is not being enforced. The agent appears to be selecting trending/viral Reddit posts regardless of subreddit, then assigning category labels independently, rather than executing the SQL filter and returning only matching rows.

**Requested fix:** When a prompt contains explicit SQL with a WHERE clause, execute it as written against the Snowflake tables. The subreddit whitelist and keyword ILIKE filters must restrict the result set before it is returned.
