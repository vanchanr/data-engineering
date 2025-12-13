# Pattern 1: Aggregation with GROUP BY - Easy Solutions

## Solution Walkthrough

This section provides SQL solutions and explanations for each problem. Understand not just the query, but the "why" behind each step.

---

## Problem 1 Solution: Twitter - Total Tweets Per User

**SQL Query:**
```sql
SELECT 
    user_id,
    COUNT(*) as tweet_count
FROM tweets
GROUP BY user_id
ORDER BY tweet_count DESC;
```

**Explanation:**
- `COUNT(*)` counts all rows for each user
- `GROUP BY user_id` groups tweets into buckets by user
- `ORDER BY tweet_count DESC` shows most prolific users first
- This is the most basic aggregation pattern

**Key Learning:**
- GROUP BY creates separate groups for each unique value
- COUNT(*) counts rows in each group
- Always include GROUP BY columns in SELECT when using aggregates

---

## Problem 2 Solution: Instagram - Total Followers Per User

**SQL Query:**
```sql
SELECT 
    user_id,
    COUNT(*) as follower_count
FROM followers
GROUP BY user_id
ORDER BY follower_count DESC
LIMIT 10;
```

**Explanation:**
- Each row in followers table represents one follower
- COUNT(*) counts how many rows (followers) exist for each user_id
- LIMIT 10 shows only the top 10 most-followed users
- This teaches filtering results after aggregation

**Key Learning:**
- LIMIT is applied AFTER aggregation and sorting
- ORDER BY + LIMIT is how you find "top N" items
- For ranking problems, always think about LIMIT

---

## Problem 3 Solution: Spotify - Average Song Duration Per Artist

**SQL Query:**
```sql
SELECT 
    artist_id,
    artist_name,
    ROUND(AVG(duration_seconds), 2) as avg_duration
FROM songs
GROUP BY artist_id, artist_name
HAVING COUNT(*) > 5
ORDER BY avg_duration DESC;
```

**Explanation:**
- `AVG(duration_seconds)` calculates average across all songs per artist
- `ROUND(..., 2)` rounds to 2 decimal places for readability
- `GROUP BY artist_id, artist_name` groups by artist
- `HAVING COUNT(*) > 5` filters out artists with 5 or fewer songs
  - **Important**: HAVING filters AFTER grouping (COUNT is computed for each group)
  - WHERE would filter BEFORE grouping (wouldn't work here)
- `ORDER BY avg_duration DESC` shows longest average song duration first

**Key Learning:**
- Use HAVING for filtering on aggregate functions
- Use WHERE for filtering on raw columns
- ORDER BY can apply to aggregated columns (aliases)
- ROUND() makes output more readable

---

## Problem 4 Solution: YouTube - Total Views Per Channel

**SQL Query:**
```sql
SELECT 
    channel_id,
    channel_name,
    SUM(views) as total_views
FROM videos
GROUP BY channel_id, channel_name
ORDER BY total_views DESC;
```

**Explanation:**
- `SUM(views)` adds up all views for each channel
- Grouping by both channel_id and channel_name ensures correct output
- This is a basic roll-up query (summarizing detail into summary)
- Real dashboards use this pattern constantly

**Key Learning:**
- SUM is used for totaling numeric columns
- When grouping by multiple columns, include all of them in SELECT
- This teaches multi-dimensional grouping

---

## Problem 5 Solution: Amazon - Number of Orders Per Customer

**SQL Query:**
```sql
SELECT 
    customer_id,
    COUNT(*) as order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) >= 3
ORDER BY order_count DESC;
```

**Explanation:**
- `COUNT(*)` counts number of rows (orders) per customer
- `HAVING COUNT(*) >= 3` keeps only customers with 3+ orders
- Why HAVING and not WHERE?
  - WHERE: `WHERE COUNT(*) >= 3` ❌ Invalid - COUNT doesn't exist yet
  - HAVING: Filters after grouping is complete ✓
- This finds your "power users"

**Key Learning:**
- HAVING is applied to aggregated results
- WHERE is applied before grouping
- Think about the sequence: WHERE → GROUP BY → HAVING → ORDER BY

---

## Problem 6 Solution: Netflix - Average Watch Duration Per User

**SQL Query:**
```sql
SELECT 
    user_id,
    ROUND(AVG(duration_minutes), 1) as avg_watch_duration
FROM watch_history
GROUP BY user_id
ORDER BY avg_watch_duration DESC
LIMIT 20;
```

**Explanation:**
- `AVG(duration_minutes)` calculates average watch time per user
- `ROUND(..., 1)` shows one decimal place
- `LIMIT 20` shows the 20 most engaged users (by watch duration)
- Real product teams use this to identify highly engaged users

**Key Learning:**
- Combining AVG with ORDER BY and LIMIT finds extreme values
- "Top 20 most engaged" is a common business question
- Use ROUND to format numeric output properly

---

## Problem 7 Solution: Uber - Total Revenue Per Driver

**SQL Query:**
```sql
SELECT 
    driver_id,
    driver_name,
    SUM(fare_amount) as total_revenue
FROM rides
GROUP BY driver_id, driver_name
HAVING SUM(fare_amount) > 1000
ORDER BY total_revenue DESC;
```

**Explanation:**
- `SUM(fare_amount)` totals earnings for each driver
- `HAVING SUM(fare_amount) > 1000` filters drivers earning $1000+
- This identifies high-earning drivers for bonus/incentive programs
- Note: In HAVING, you can repeat the aggregate function

**Key Learning:**
- SUM with HAVING filters based on totals
- In HAVING, you can use the aggregate function directly (SUM, COUNT, etc.)
- This pattern is used for threshold-based filtering (revenue, count, etc.)

---

## Problem 8 Solution: Google Drive - Files Per User Per Folder

**SQL Query:**
```sql
SELECT 
    user_id,
    folder_id,
    COUNT(*) as file_count
FROM files
GROUP BY user_id, folder_id
ORDER BY user_id, file_count DESC;
```

**Explanation:**
- `GROUP BY user_id, folder_id` creates combinations of user + folder
- For example:
  - User 1 in Folder A: 5 files
  - User 1 in Folder B: 3 files
  - User 2 in Folder A: 7 files
- This is a 2D aggregation (by user AND by folder)
- Real-world: Storage dashboards use this to show per-folder usage

**Key Learning:**
- GROUP BY can have multiple columns for multi-dimensional analysis
- All GROUP BY columns must appear in SELECT (or be in aggregates)
- ORDER BY can reference original GROUP BY columns (user_id) and aggregates (file_count)

---

## Problem 9 Solution: LinkedIn - Total Endorsements Per Skill

**SQL Query:**
```sql
SELECT 
    skill_id,
    skill_name,
    COUNT(*) as endorsement_count
FROM endorsements
GROUP BY skill_id, skill_name
HAVING COUNT(*) >= 100
ORDER BY endorsement_count DESC;
```

**Explanation:**
- `COUNT(*)` counts each endorsement row
- `GROUP BY skill_id, skill_name` groups by skill
- `HAVING COUNT(*) >= 100` shows only popular skills
- This identifies trending skills on the platform
- Useful for: showing "trending skills" feature

**Key Learning:**
- COUNT combined with HAVING finds "popular" items (>threshold)
- This pattern works for trending topics, popular items, etc.
- The threshold is business-driven (why 100? That's configurable)

---

## Problem 10 Solution: WhatsApp - Average Messages Per Conversation

**SQL Query:**
```sql
SELECT 
    conversation_id,
    ROUND(AVG(message_length), 0) as avg_message_length
FROM messages
GROUP BY conversation_id
ORDER BY avg_message_length DESC
LIMIT 15;
```

**Explanation:**
- `AVG(message_length)` calculates average message size per conversation
- `ROUND(..., 0)` rounds to whole number (no decimals)
- `ORDER BY avg_message_length DESC` sorts by most verbose conversations
- `LIMIT 15` shows top 15 conversations
- Real-world: Identifies which conversations have longer, more detailed messages

**Key Learning:**
- Combining multiple aggregation operations: AVG + ROUND + ORDER BY + LIMIT
- This is the most complete aggregation query combining multiple techniques
- Order of operations matters: GROUP BY → HAVING → ORDER BY → LIMIT

---

## Pattern 1 Summary: Key Takeaways

### When to Use Aggregation with GROUP BY:
✅ Calculating metrics (totals, averages, counts)
✅ Creating dashboards and KPI reports
✅ Analyzing data by segments (by user, by category, by time period)
✅ Finding top/bottom items (top users, least popular products)
✅ Understanding data distribution

### Common Aggregate Functions:
| Function | Use Case | Example |
|----------|----------|---------|
| COUNT(*) | Count rows | How many tweets per user |
| COUNT(column) | Count non-null values | Count users with posts |
| SUM() | Total | Total revenue |
| AVG() | Average | Average order value |
| MIN() | Minimum | Lowest price |
| MAX() | Maximum | Highest rating |

### Query Execution Order:
1. **FROM** - Choose table
2. **WHERE** - Filter rows BEFORE grouping
3. **GROUP BY** - Create groups
4. **HAVING** - Filter groups AFTER aggregation
5. **ORDER BY** - Sort results
6. **LIMIT** - Limit rows returned

### Common Mistakes to Avoid:
❌ Using WHERE when you should use HAVING (for aggregate filters)
❌ Forgetting to include GROUP BY columns in SELECT
❌ Using COUNT(column) when COUNT(*) is simpler
❌ Forgetting ROUND() for decimal numbers (hard to read)
❌ Not using HAVING for filtering on aggregate results

