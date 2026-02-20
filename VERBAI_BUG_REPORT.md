# VerbAI MCP — Bug Report
**Date:** 2026-02-20
**Reported by:** SOTU Dashboard (rocketago/sotu-dashboard)
**Integration:** VerbAI MCP via Claude CLI (`claude --print --dangerously-skip-permissions`)
**Tables queried:** `SEARCH_EVENTS_FLAT_DYM`, `REDDIT_EVENTS_FLAT_DYM`, `AGENT_SYNC`

---

## Background

The SOTU Dashboard queries VerbAI to surface political engagement data for users aged 18–29. The panel is ~3,000 users, the majority of whom fall in the 18–29 age bracket. Two separate queries are affected: one for aggregate category counts (`fetch_category_counts`) and one for a real-time individual event feed (`fetch_live_events`). Both are broken in distinct ways described below.

---

## Bug 1 — Live event feed ignores the political keyword filter entirely

### What we request

The prompt instructs VerbAI to run this SQL (simplified):

```sql
SELECT s.EVENT_TIME, s.QUERY, 'search' AS source, ...
FROM SEARCH_EVENTS_FLAT_DYM s
JOIN AGENT_SYNC a ON s.USER_ID = a.USER_ID
WHERE s.EVENT_TIME >= '2026-02-19T02:23:08Z'
  AND (YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH) BETWEEN 18 AND 29
  AND (
    s.QUERY ILIKE '%trump%' OR s.QUERY ILIKE '%congress%' OR
    s.QUERY ILIKE '%president%' OR s.QUERY ILIKE '%election%' OR
    s.QUERY ILIKE '%immigration%' OR s.QUERY ILIKE '%tariff%' OR
    s.QUERY ILIKE '%ukraine%' OR s.QUERY ILIKE '%israel%' OR
    s.QUERY ILIKE '%climate%' OR s.QUERY ILIKE '%healthcare%' OR
    s.QUERY ILIKE '%federal%' OR s.QUERY ILIKE '%doge%' OR
    s.QUERY ILIKE '%elon musk%' OR s.QUERY ILIKE '%deportation%' OR
    s.QUERY ILIKE '%war%' -- ... ~38 political keyword conditions total
  )
ORDER BY s.EVENT_TIME DESC LIMIT 50;
```

Expected result: up to 50 recent search events where the query matches at least one of ~38 political keywords, from users aged 18–29.

### What we actually receive

```
50 events returned.
Category breakdown: { "entertainment": 36, "sports": 14 }
Unique users: 1  (a single 24-year-old female from IL)
```

Sample events returned:

| time | query | category |
|------|-------|----------|
| 2026-02-19T18:13:04Z | `dmd friendship season 3` | entertainment |
| 2026-02-19T18:13:04Z | `junhwan cha olympics 2026` | sports |
| 2026-02-19T18:13:04Z | `pyeongchang 2018 figure skating` | sports |
| 2026-02-19T18:13:04Z | `yuzuru hanyu 2022 brijing olympics gala performance` | sports |
| 2026-02-19T18:13:04Z | `unethical faouzia` | entertainment |

### What this indicates

- The `QUERY ILIKE '%trump%' OR ...` WHERE clause is not being applied. None of the 50 returned queries match any of the ~38 political keywords.
- All 50 events belong to a single user — the query appears to be returning the full recent search history of one user rather than aggregating across the panel with the specified filters.
- The `category` field is being assigned freely by the response (returning "entertainment", "sports") rather than from the canonical list we specify in the prompt.

---

## Bug 2 — Category engagement counts return Reddit scores instead of VerbAI user engagement

### What we request

The prompt instructs VerbAI to count how many VerbAI users (aged 18–29) searched for or engaged with political content today, grouped by policy category. The intent is:

- `engagement_count` = number of search/post events by VerbAI panel users
- `unique_users` = number of distinct VerbAI panel users

With ~3,000 panel users (majority 18–29), we would expect at minimum several hundred unique users and thousands of engagement events on a typical day for high-salience political categories like Presidential Politics.

### What we actually receive

```
generated_at: [field missing from response]

Categories with data (3 of 11):
  General Politics:      17 users,     108,644 engagements
  Foreign Policy:         2 users,      16,378 engagements
  Environmental Policy:   1 user,        3,647 engagements

Categories with zero data (8 of 11):
  Presidential Politics, Elections & Voting, Immigration Policy,
  Legislative Politics, Economic Policy, Healthcare Policy,
  Education Policy, Civil Rights
```

Sample items returned under "General Politics":

| source | count | subreddit | query |
|--------|-------|-----------|-------|
| reddit | 23,789 | r/cats | `came home and my cats feet are yellow?` |
| reddit | 19,328 | r/pics | `Andrew Mountbatten Windsor has been arrested on suspicion of misconduct in public office` |
| reddit | 12,426 | r/memes | `They were real Chads` |

Sample items returned under "Foreign Policy":

| source | count | subreddit | query |
|--------|-------|-----------|-------|
| reddit | 9,455 | r/interestingasfuck | `Bro went to space just to never return to his "country" cause "country" was gone` |
| reddit | 6,923 | r/worldnews | `Former South Korean President Yoon Sentenced to Life in Prison for Coup Attempt` |

### What this indicates

1. **`count` is a Reddit post score (upvotes), not a VerbAI user engagement count.** Values of 23,789 engagements from 17 users would require ~1,400 search events per user in a single day — impossible. These are Reddit upvote scores being passed through as the engagement metric.

2. **The political keyword filter is not being applied here either.** `r/cats` asking about yellow cat feet and `r/memes` with "They were real Chads" are not political queries under any interpretation. The subreddit allowlist (`politics`, `worldnews`, `news`, etc.) and the title `ILIKE` conditions are not being enforced.

3. **The `AGENT_SYNC` join for age filtering does not appear to be executing.** If it were, the results would reflect VerbAI panel behaviour, not global Reddit popularity. The data looks like a general Reddit trending-posts feed rather than VerbAI user activity.

4. **8 of 11 categories return zero data**, including Presidential Politics — the category most likely to have the highest real-world engagement. This is inconsistent with expected panel behaviour during a politically active news cycle.

5. **`generated_at` is absent** from the response, suggesting the structured output contract is not being fully honoured.

---

## Bug 3 — AGENT_SYNC user count query does not return

### Context

The panel is reported to contain approximately 3,000 users, the majority of whom are aged 18–29. If accurate, this age cohort likely represents ~2,000–2,500 users. That figure makes the results in Bug 1 and Bug 2 even harder to explain: a single user in the live feed (Bug 1) and only 20 unique users across all political categories (Bug 2) would represent less than 1% of expected panel activity.

### Validation attempt

To confirm the true 18–29 user count and rule out a small panel as the explanation, we issued the following query directly:

```sql
SELECT
  COUNT(*) AS total_users,
  SUM(CASE WHEN (YEAR(CURRENT_DATE) - YEAR_OF_BIRTH) BETWEEN 18 AND 29 THEN 1 ELSE 0 END) AS users_18_29,
  MIN(YEAR(CURRENT_DATE) - YEAR_OF_BIRTH) AS min_age,
  MAX(YEAR(CURRENT_DATE) - YEAR_OF_BIRTH) AS max_age
FROM AGENT_SYNC;
```

### Result

**The query did not return after 30+ minutes and was killed.** No response, no error, no partial output.

### What this indicates

This is an additional failure mode separate from Bugs 1 and 2. Either:

- `AGENT_SYNC` does not exist under that name, causing the query to hang rather than fail fast
- The table exists but querying it via the MCP integration is broken in a way that produces no response rather than an error
- There is a permissions or connectivity issue specific to `AGENT_SYNC` that does not affect `SEARCH_EVENTS_FLAT_DYM` and `REDDIT_EVENTS_FLAT_DYM` (which do return results, albeit incorrect ones)

The fact that `AGENT_SYNC` is referenced in every JOIN in Bugs 1 and 2 — and may not exist or be accessible — could also explain why those queries are returning data without the age filter applied: if the JOIN silently fails or is dropped, the query would return unfiltered results from the event tables.

---

## Questions for VerbAI

1. **Schema confirmation:** Can you confirm the correct table and column names for search events, Reddit events, and the user demographic table? Specifically: are `SEARCH_EVENTS_FLAT_DYM`, `REDDIT_EVENTS_FLAT_DYM`, and `AGENT_SYNC` the correct names, and are the column names `QUERY`, `TITLE`, `USER_ID`, `EVENT_TIME`, `YEAR_OF_BIRTH`, `FULL_ADDRESS`, `SUBREDDIT` correct?

2. **SQL execution:** Are the SQL WHERE clauses in our prompts being executed as literal Snowflake SQL, or are they being interpreted as natural language intent? If the latter, is there a preferred way to submit structured queries that guarantees filter application?

3. **`engagement_count` definition:** What does the `count`/`engagement_count` field represent in the context of VerbAI panel data? Is it the number of VerbAI users who performed the action, the total event count, or a score sourced from the upstream platform (e.g. Reddit upvotes)?

4. **`AGENT_SYNC` accessibility:** Does the `AGENT_SYNC` table exist under that name and is it accessible via this integration? A direct `SELECT COUNT(*) FROM AGENT_SYNC` query hung for 30+ minutes without returning. If the table name is wrong or the table is inaccessible, this would explain why the age JOIN in Bugs 1 and 2 is silently not being applied.

5. **Age filter:** If `AGENT_SYNC` is the correct table, is `YEAR(CURRENT_DATE) - a.YEAR_OF_BIRTH` the correct expression for age? Or is there a pre-computed age column?

6. **Panel coverage:** The panel is reported to be ~3,000 users, majority aged 18–29 (~2,000–2,500 users in scope). Given that, is it expected that a query for political searches today would return results from only a single user? Or does this indicate a query execution issue?

---

## Update — 2026-02-20 Run: `fetch_search_queries` also returned non-political content

This is a new observation from the **2026-02-20 action run** (workflow: Auto-update political data).

### What we requested

`fetch_search_queries` prompt (simplified) — the full text is in `fetch_data.py:fetch_search_queries()`:

```sql
-- QUERY 2 (Reddit posts) — key WHERE clause:
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

Expected: top Reddit posts from political subreddits (or with political titles), by 18–29 VerbAI users.

### What was returned

The items written to `political_data.json` (git commit `a0dd54f` preserves the file) included:

| subreddit | count | query | Political? |
|---|---|---|---|
| `r/cats` | 23,789 | `came home and my cats feet are yellow?` | ❌ Not in whitelist, no keyword |
| `r/memes` | 12,426 | `They were real Chads` | ❌ Not in whitelist, no keyword |
| `r/pcmasterrace` | 12,184 | `discord right now:` | ❌ Not in whitelist, no keyword |
| `r/losercity` | 4,377 | `Kitty's first date` | ❌ Not in whitelist, no keyword |
| `r/losercity` | 3,744 | `Losercity Quake memes` | ❌ Not in whitelist, no keyword |
| `r/losercity` | 3,619 | `Losercity gluten` | ❌ Not in whitelist, no keyword |
| `r/nbacirclejerk` | 1,557 | `Barely into June and she's already acting out` | ❌ Sports, not politics |
| `r/worldnews` | 6,923 | `Former South Korean President Yoon Sentenced to Life in Prison` | ✅ |
| `r/interestingasfuck` | 9,455 | `Bro went to space just to never return to his "country"` | ⚠️ Borderline |

9 of ~17 Reddit items were from subreddits not in the whitelist and had no matching political keywords.
All were categorised as `"General Politics"` by the agent.

### Additional finding — `last_mcp_pull: null` despite data being written

The `fetch_category_counts` call (which asks for aggregate engagement by policy_category)
returned **0 rows** in this run, while `fetch_search_queries` returned ~70 rows.
Both calls hit the same Snowflake tables via the same MCP session in the same workflow run.
This confirms the agent is not consistently executing both SQL statements against live data.

### Conclusion

The `LOWER(r.SUBREDDIT) IN (...)` whitelist in QUERY 2 is not being enforced by the agent.
The agent appears to be selecting trending/viral Reddit posts regardless of subreddit,
then assigning category labels independently (defaulting to "General Politics" for anything
it cannot classify), rather than executing the SQL as written and returning only matching rows.

---

## Reproduction

All queries are issued via:

```bash
claude --print --dangerously-skip-permissions "<prompt>"
```

with the VerbAI MCP registered. The full prompt text for each query is in `fetch_data.py` in functions `fetch_category_counts()` (line ~401) and `fetch_live_events()` (line ~569) of the repository.

The current broken output is in `live_feed.json` (generated 2026-02-20T02:23:08Z) and `political_data.json` in the repo root.
