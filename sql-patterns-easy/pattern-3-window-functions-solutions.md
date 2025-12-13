# Pattern 3: Ranking with Window Functions - Easy Solutions

## Solution Walkthrough

Window functions are the most powerful and most-tested pattern in FAANG interviews. The key is understanding how PARTITION BY and ORDER BY work together.

---

## Problem 1 Solution: Twitter - Top 3 Most Liked Tweets Per User

**SQL Query:**
```sql
SELECT 
    user_id,
    tweet_id,
    likes_count,
    ROW_NUMBER() OVER (
        PARTITION BY user_id 
        ORDER BY likes_count DESC
    ) as tweet_rank
FROM tweets
WHERE ROW_NUMBER() OVER (
    PARTITION BY user_id 
    ORDER BY likes_count DESC
) <= 3;
```

**Better Query (Using CTE - Cleaner):**
```sql
WITH ranked_tweets AS (
    SELECT 
        user_id,
        tweet_id,
        likes_count,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY likes_count DESC
        ) as tweet_rank
    FROM tweets
)
SELECT *
FROM ranked_tweets
WHERE tweet_rank <= 3
ORDER BY user_id, tweet_rank;
```

**Explanation:**
- `ROW_NUMBER() OVER (...)` assigns sequential numbers within each partition
- `PARTITION BY user_id` creates separate rankings for each user
- `ORDER BY likes_count DESC` ranks by likes (highest first, so highest rank = 1)
- ROW_NUMBER gives 1, 2, 3, 4... even if values are tied
- The WHERE clause filters to only keep rank <= 3

**Key Learning:**
- Window functions don't combine rows (unlike GROUP BY)
- Each row retains its original data + the window function result
- Use CTE to make code cleaner (compute window function, then filter)
- This "top N per group" is THE most common interview pattern

---

## Problem 2 Solution: Instagram - Top Post Per Day

**SQL Query:**
```sql
WITH ranked_posts AS (
    SELECT 
        post_id,
        created_date,
        likes_count,
        ROW_NUMBER() OVER (
            PARTITION BY created_date 
            ORDER BY likes_count DESC
        ) as daily_rank
    FROM posts
)
SELECT 
    created_date,
    post_id,
    likes_count,
    daily_rank
FROM ranked_posts
WHERE daily_rank = 1
ORDER BY created_date DESC;
```

**Explanation:**
- Partitions by day, so each day gets its own ranking
- ROW_NUMBER by likes DESC means highest likes = rank 1
- WHERE daily_rank = 1 gets only the best post per day
- This is the "top 1 per group" pattern (very common)

**Key Learning:**
- When filtering to WHERE rank = 1, use CTE or subquery
- Can't use window functions directly in WHERE (they're computed after WHERE)
- This pattern: "best item per category/day/region/etc."

---

## Problem 3 Solution: Spotify - User's Most Played Songs Ranking

**SQL Query:**
```sql
SELECT 
    user_id,
    song_name,
    play_count,
    RANK() OVER (
        PARTITION BY user_id 
        ORDER BY play_count DESC
    ) as rank
FROM listening_history
ORDER BY user_id, rank;
```

**Explanation:**
- `RANK()` assigns ranks, but repeats the rank for ties
- Example: If two songs have 100 plays, both get rank 1, next gets rank 3
- `PARTITION BY user_id` means each user's songs are ranked independently
- Unlike ROW_NUMBER, RANK handles ties realistically
- If two songs have same plays, they have the same rank

**Key Learning:**
- **ROW_NUMBER**: 1, 2, 3, 4... (no ties)
- **RANK**: 1, 1, 3, 4... (ties share rank, next skips)
- **DENSE_RANK**: 1, 1, 2, 3... (ties share rank, next continuous)

**When to Use Each:**
- ROW_NUMBER: Top N items (1st place goes to first row if tied)
- RANK: When ties matter (both are equally ranked)
- DENSE_RANK: When gaps don't matter (continuous ranking)

---

## Problem 4 Solution: YouTube - Top 5 Channels Per Country

**SQL Query:**
```sql
WITH ranked_channels AS (
    SELECT 
        channel_id,
        channel_name,
        country,
        subscriber_count,
        ROW_NUMBER() OVER (
            PARTITION BY country 
            ORDER BY subscriber_count DESC
        ) as country_rank
    FROM channels
)
SELECT 
    country,
    channel_id,
    channel_name,
    subscriber_count,
    country_rank
FROM ranked_channels
WHERE country_rank <= 5
ORDER BY country, country_rank;
```

**Explanation:**
- `PARTITION BY country` creates separate rankings per country
- `ORDER BY subscriber_count DESC` largest channels first
- WHERE country_rank <= 5 keeps top 5 per country
- This finds "top 5 items per group"

**Key Learning:**
- PARTITION BY can be any column (category, region, user, etc.)
- Multiple partitions create independent rankings
- This is used for: top products per category, best employees per department, etc.

---

## Problem 5 Solution: Amazon - Customer's Top 5 Purchases

**SQL Query:**
```sql
WITH ranked_orders AS (
    SELECT 
        customer_id,
        order_id,
        order_amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY order_amount DESC
        ) as order_rank
    FROM orders
)
SELECT 
    customer_id,
    order_id,
    order_amount,
    order_rank
FROM ranked_orders
WHERE order_rank <= 5
ORDER BY customer_id, order_rank;
```

**Explanation:**
- Ranks each customer's orders by amount (highest first)
- Keeps top 5 orders per customer
- Real-world: Identifies biggest orders from each customer
- Can be used for: detecting VIPs, finding biggest transactions, etc.

**Key Learning:**
- ROW_NUMBER gives each row a unique number even if values repeat
- This pattern: "top N per person/company/segment"

---

## Problem 6 Solution: Netflix - Latest Watched Per User Per Genre

**SQL Query:**
```sql
WITH ranked_content AS (
    SELECT 
        wh.user_id,
        wh.genre,
        c.title,
        wh.watch_date,
        ROW_NUMBER() OVER (
            PARTITION BY wh.user_id, wh.genre 
            ORDER BY wh.watch_date DESC
        ) as recency_rank
    FROM watch_history wh
    INNER JOIN content c ON wh.content_id = c.content_id
)
SELECT 
    user_id,
    genre,
    title,
    watch_date
FROM ranked_content
WHERE recency_rank = 1
ORDER BY user_id, watch_date DESC;
```

**Explanation:**
- `PARTITION BY user_id, genre` creates separate rankings for each user-genre combo
- Example: User 1's latest Drama, User 1's latest Action, User 2's latest Drama...
- `ORDER BY watch_date DESC` most recent first
- WHERE recency_rank = 1 keeps only most recent per user-genre
- This is multi-dimensional partitioning

**Key Learning:**
- PARTITION BY can have multiple columns
- This creates nested grouping: each user, within each user's genres
- Real-world: "most recently watched [type] for [person]"

---

## Problem 7 Solution: Uber - Driver's Highest-Rated Rides

**SQL Query:**
```sql
WITH ranked_rides AS (
    SELECT 
        driver_id,
        ride_id,
        rating,
        ROW_NUMBER() OVER (
            PARTITION BY driver_id 
            ORDER BY rating DESC
        ) as ride_rank
    FROM rides
)
SELECT 
    driver_id,
    ride_id,
    rating,
    ride_rank
FROM ranked_rides
WHERE ride_rank <= 3
ORDER BY driver_id, ride_rank;
```

**Explanation:**
- Ranks each driver's rides by rating (highest first)
- Top 3 rides per driver (ride_rank <= 3)
- Real-world: Quality metrics, driver performance
- Can identify: best-performing drivers, quality issues, etc.

**Key Learning:**
- This is the core "top N per group" pattern
- Appears in almost every domain: products per seller, employees per department, etc.

---

## Problem 8 Solution: LinkedIn - Top Skill Per User

**SQL Query:**
```sql
WITH ranked_skills AS (
    SELECT 
        user_id,
        skill_name,
        endorsement_count,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY endorsement_count DESC
        ) as skill_rank
    FROM user_skills
)
SELECT 
    user_id,
    skill_name,
    endorsement_count
FROM ranked_skills
WHERE skill_rank = 1
ORDER BY user_id;
```

**Explanation:**
- For each user, ranks skills by endorsements
- Filters to only the #1 skill per user
- If two skills tied for most endorsements, only one is shown (arbitrary choice by ROW_NUMBER)
- If tie matters, use DENSE_RANK instead

**Key Learning:**
- "Top 1" queries should use WHERE rank = 1
- This pattern: "best item per person", "favorite per user", etc.

---

## Problem 9 Solution: Google Drive - Largest File Per Folder

**SQL Query:**
```sql
WITH ranked_files AS (
    SELECT 
        folder_id,
        file_id,
        file_name,
        file_size_mb,
        ROW_NUMBER() OVER (
            PARTITION BY folder_id 
            ORDER BY file_size_mb DESC
        ) as size_rank
    FROM files
)
SELECT 
    folder_id,
    file_id,
    file_name,
    file_size_mb
FROM ranked_files
WHERE size_rank = 1
ORDER BY folder_id;
```

**Explanation:**
- Ranks files within each folder by size
- Filters to only the largest file per folder
- Real-world: Storage optimization, finding space hogs
- Could also use: ORDER BY folder_id, file_size_mb DESC LIMIT 1 per folder (more complex)

**Key Learning:**
- Window functions are cleaner than alternatives for this pattern
- Real use case: identifying which files take up most space per location

---

## Problem 10 Solution: WhatsApp - Most Active Conversation Per User

**SQL Query:**
```sql
WITH ranked_conversations AS (
    SELECT 
        user_id,
        conversation_id,
        message_count,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY message_count DESC
        ) as activity_rank
    FROM messages
)
SELECT 
    user_id,
    conversation_id,
    message_count
FROM ranked_conversations
WHERE activity_rank = 1
ORDER BY user_id;
```

**Explanation:**
- For each user, finds their most-active conversation (by message count)
- This is "busiest conversation per person"
- Real-world: Engagement metrics, user behavior analysis
- Can identify: closest friends, most important conversations, etc.

**Key Learning:**
- This pattern works for any "most X per person" question
- Can identify: best product per seller, busiest hour per day, etc.

---

## Pattern 3 Summary: Key Takeaways

### Window Functions Overview:
| Function | Output | Use Case |
|----------|--------|----------|
| ROW_NUMBER() | 1, 2, 3, 4... | Top N items |
| RANK() | 1, 1, 3, 4... | Ties with gaps |
| DENSE_RANK() | 1, 1, 2, 3... | Ties without gaps |
| LAG() | Previous row value | Comparing consecutive rows |
| LEAD() | Next row value | Looking ahead |
| FIRST_VALUE() | First row in window | First item in group |
| LAST_VALUE() | Last row in window | Last item in group |

### The OVER Clause:
```sql
FUNCTION() OVER (
    PARTITION BY column1, column2  -- Creates separate rankings
    ORDER BY column3 DESC           -- Order within partition
    ROWS BETWEEN ... AND ...        -- Optional: frame specification
)
```

### Most Common Interview Pattern: Top N Per Group

**Template:**
```sql
WITH ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY group_column 
            ORDER BY rank_column DESC
        ) as rank
    FROM table
)
SELECT * FROM ranked
WHERE rank <= N;
```

### When to Use Window Functions vs Alternatives:

**Use Window Functions When:**
✅ You need to keep original row data with rankings
✅ You need "top N per group"
✅ You need to compare adjacent rows
✅ You need running totals/cumulative sums

**Use GROUP BY When:**
✅ You only need summarized data
✅ You don't need individual row details
✅ Simpler query, potentially better performance

### Common Mistakes to Avoid:
❌ Trying to use window function in WHERE clause (won't work - use CTE or subquery)
❌ Forgetting PARTITION BY (ranks entire dataset, not per group)
❌ Wrong ORDER BY direction (DESC = 1 is best)
❌ Using ROW_NUMBER for everything (consider RANK/DENSE_RANK for ties)
❌ Not using CTE for clarity when filtering on window function

### Practice Variations:
Try these variations to deepen understanding:
- Change PARTITION BY (by user, by category, by date)
- Change ORDER BY (by amount, by date, by count)
- Change the filter (top 1, top 5, top 10%)
- Use RANK instead of ROW_NUMBER
- Combine multiple window functions

