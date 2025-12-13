# Pattern 4: Moving Averages & Cumulative Sums - Easy Solutions

## Solution Walkthrough

Time-series patterns use window functions with frame clauses. The key is understanding ROWS BETWEEN and ORDER BY for time-based calculations.

---

## Problem 1 Solution: Twitter - Cumulative Tweet Count Per User

**SQL Query:**
```sql
SELECT 
    created_date,
    COUNT(*) as daily_tweet_count,
    SUM(COUNT(*)) OVER (
        ORDER BY created_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_tweets
FROM tweets
WHERE user_id = 1
GROUP BY created_date
ORDER BY created_date;
```

**Explanation:**
- `COUNT(*)` counts tweets per day (with GROUP BY)
- `SUM(...) OVER (...)` creates a cumulative sum
- `ORDER BY created_date` sorts chronologically
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` means:
  - Include all rows from start up to current row
  - This creates cumulative effect
- Example output:
  ```
  Date       Daily   Cumulative
  2024-01-01   5         5
  2024-01-02   3         8
  2024-01-03   7        15
  ```

**Key Learning:**
- Cumulative = running total from start to now
- Use SUM() OVER with ROWS BETWEEN
- UNBOUNDED PRECEDING = from beginning
- CURRENT ROW = up to current row

---

## Problem 2 Solution: Instagram - 7-Day Rolling Average of Likes

**SQL Query:**
```sql
SELECT 
    metric_date,
    total_likes,
    ROUND(AVG(total_likes) OVER (
        ORDER BY metric_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as moving_avg_7day
FROM daily_metrics
ORDER BY metric_date;
```

**Explanation:**
- `AVG(...) OVER (...)` creates a moving average
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` means:
  - Include current row + 6 previous rows = 7 days total
  - First 6 rows will have fewer than 7 days
- Example:
  ```
  Date    Likes  7-Day Avg
  Day 1     100    100.00      (only 1 day available)
  Day 2     120    110.00      (2 days available)
  ...
  Day 7     140    120.00      (7 days available)
  Day 8     130    125.00      (still 7: days 2-8)
  Day 9     125    120.00      (still 7: days 3-9)
  ```

**Key Learning:**
- Moving Average = average of last N rows
- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW = 7 day window
- Older rows have fewer available days (handles edge case nicely)
- ROUND(..., 2) formats to 2 decimal places

---

## Problem 3 Solution: Spotify - Cumulative Streams Per Artist (Monthly)

**SQL Query:**
```sql
SELECT 
    month,
    stream_count as monthly_streams,
    SUM(stream_count) OVER (
        PARTITION BY artist_id 
        ORDER BY month 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_streams
FROM monthly_streams
WHERE artist_id = 5
ORDER BY month;
```

**Explanation:**
- `PARTITION BY artist_id` keeps each artist separate
- `ORDER BY month` sorts chronologically
- `SUM(...) OVER (...)` with ROWS BETWEEN creates running total
- UNBOUNDED PRECEDING = from first month ever
- Example:
  ```
  Month    Streams  Cumulative
  Jan          50K      50K
  Feb          60K     110K
  Mar          55K     165K
  Apr          70K     235K
  ```

**Key Learning:**
- Cumulative sums work with any time period (daily, monthly, yearly)
- PARTITION BY + ORDER BY = per-artist time series
- This is critical for: revenue tracking, growth analysis, retention curves

---

## Problem 4 Solution: YouTube - 30-Day Rolling Average Views

**SQL Query:**
```sql
SELECT 
    view_date,
    view_count,
    ROUND(AVG(view_count) OVER (
        ORDER BY view_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 0) as moving_avg_30day
FROM daily_channel_views
WHERE channel_id = 10
ORDER BY view_date DESC
LIMIT 60;
```

**Explanation:**
- `ROWS BETWEEN 29 PRECEDING AND CURRENT ROW` = 30 day window
- 29 previous rows + current = 30 days total
- `ORDER BY view_date DESC` then `LIMIT 60` shows most recent 60 days
- Example:
  ```
  Date     Views   30-Day Avg
  Day 60   1000     980 (average of last 30)
  Day 59    950     970
  ...
  Day 1     500     500 (only 1 day available)
  ```

**Key Learning:**
- 30-day rolling average = "smooth out spikes"
- Common in dashboards to show trends, not daily noise
- ROUND(..., 0) removes decimals for view counts

---

## Problem 5 Solution: Amazon - Cumulative Revenue Per Day

**SQL Query:**
```sql
WITH daily_revenue AS (
    SELECT 
        order_date,
        SUM(order_amount) as daily_revenue
    FROM orders
    WHERE order_date >= CURDATE() - INTERVAL 30 DAY
    GROUP BY order_date
)
SELECT 
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue
FROM daily_revenue
ORDER BY order_date;
```

**Explanation:**
- CTE first aggregates to daily total
- Then applies cumulative sum
- `WHERE order_date >= CURDATE() - INTERVAL 30 DAY` limits to last 30 days
- Example:
  ```
  Date      Daily Revenue  Cumulative
  2024-01-01   $10,000      $10,000
  2024-01-02   $12,000      $22,000
  2024-01-03   $15,000      $37,000
  ```
- This is THE pattern for revenue dashboards

**Key Learning:**
- Combine GROUP BY with window functions using CTE
- This is how dashboards show "revenue to date"
- Important for: tracking goals, forecasting, understanding growth

---

## Problem 6 Solution: Netflix - 14-Day Rolling Average Watch Hours

**SQL Query:**
```sql
SELECT 
    watch_date,
    total_watch_hours as daily_hours,
    ROUND(AVG(total_watch_hours) OVER (
        ORDER BY watch_date 
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ), 1) as moving_avg_14day
FROM daily_watch_hours
WHERE watch_date >= CURDATE() - INTERVAL 90 DAY
ORDER BY watch_date DESC;
```

**Explanation:**
- `ROWS BETWEEN 13 PRECEDING AND CURRENT ROW` = 14 day window
- 13 previous + current = 14 total
- WHERE filters to last 90 days
- ORDER BY DESC LIMIT 90 shows most recent
- This smooths out weekday/weekend variations

**Key Learning:**
- 14-day average is common for weekly patterns
- Smooths out day-of-week effects (weekends different from weekdays)
- Works for: engagement metrics, subscriber hours, activity trends

---

## Problem 7 Solution: Uber - Cumulative Rides Per Driver (Daily)

**SQL Query:**
```sql
SELECT 
    ride_date,
    ride_count as daily_rides,
    SUM(ride_count) OVER (
        PARTITION BY driver_id 
        ORDER BY ride_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_rides
FROM rides
WHERE driver_id = 42
ORDER BY ride_date;
```

**Explanation:**
- PARTITION BY driver_id keeps each driver separate
- Shows cumulative rides per driver over time
- Useful for: tracking driver productivity, bonus calculations
- Example:
  ```
  Date     Daily  Cumulative
  Jan 1      5       5
  Jan 2      6      11
  Jan 3      4      15
  ```

**Key Learning:**
- Cumulative with PARTITION BY is "total for this driver to date"
- Used for: bonus calculations, incentive programs, productivity tracking

---

## Problem 8 Solution: LinkedIn - 30-Day Cumulative Endorsements Per Skill

**SQL Query:**
```sql
SELECT 
    endorsement_date,
    endorsement_count as daily_count,
    SUM(endorsement_count) OVER (
        PARTITION BY skill_id 
        ORDER BY endorsement_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_count
FROM endorsements
WHERE skill_id = 123
AND endorsement_date >= CURDATE() - INTERVAL 30 DAY
ORDER BY endorsement_date;
```

**Explanation:**
- Tracks skill endorsement growth over time
- Cumulative shows: "total endorsements to date for this skill"
- Real-world: LinkedIn shows trending skills (rapid cumulative growth)
- Example:
  ```
  Date     Daily  Cumulative
  Jan 1      10       10
  Jan 2      15       25
  Jan 3      12       37
  Jan 4      20       57
  ```

**Key Learning:**
- Skills with steep cumulative curves are "trending"
- Cumulative is better than daily for seeing real growth
- This pattern: identifying trending items

---

## Problem 9 Solution: Google Drive - 7-Day Rolling Uploads Per User

**SQL Query:**
```sql
SELECT 
    upload_date,
    upload_count as daily_uploads,
    ROUND(AVG(upload_count) OVER (
        PARTITION BY user_id 
        ORDER BY upload_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1) as moving_avg_7day
FROM file_uploads
WHERE user_id = 99
AND upload_date >= CURDATE() - INTERVAL 30 DAY
ORDER BY upload_date;
```

**Explanation:**
- Shows 7-day rolling average per user
- PARTITION BY keeps each user separate
- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW = 7 days
- Example:
  ```
  Date     Daily  7-Day Avg
  Day 1      10     10.0
  Day 2      12     11.0
  ...
  Day 7      15     12.3
  Day 8      14     12.9 (average of days 2-8)
  ```

**Key Learning:**
- Rolling averages smooth out daily variations
- Useful for: detecting activity patterns, onboarding analysis
- PARTITION BY user_id works for multiple users

---

## Problem 10 Solution: WhatsApp - Cumulative Messages Per Conversation (Weekly)

**SQL Query:**
```sql
SELECT 
    week_start_date,
    message_count as weekly_messages,
    SUM(message_count) OVER (
        PARTITION BY conversation_id 
        ORDER BY week_start_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_messages
FROM weekly_messages
WHERE conversation_id = 'CONV_001'
ORDER BY week_start_date;
```

**Explanation:**
- Shows cumulative messages per conversation over weeks
- Useful for: conversation health, engagement analysis
- Example:
  ```
  Week   Weekly  Cumulative
  W1        100      100
  W2         85      185
  W3        120      305
  W4         95      400
  ```

**Key Learning:**
- Same pattern works for any time period (daily, weekly, monthly)
- PARTITION BY conversation_id separates conversations
- Cumulative shows: "total conversation activity to date"

---

## Pattern 4 Summary: Key Takeaways

### Frame Specification (ROWS BETWEEN):

```sql
ROWS BETWEEN [start] AND [end]
```

| Start/End | Meaning |
|-----------|---------|
| UNBOUNDED PRECEDING | From the very first row |
| N PRECEDING | N rows before current |
| CURRENT ROW | The current row |
| N FOLLOWING | N rows after current |
| UNBOUNDED FOLLOWING | To the very last row |

### Common Patterns:

**Cumulative Sum:**
```sql
SUM(column) OVER (
    ORDER BY date_column
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

**7-Day Moving Average:**
```sql
AVG(column) OVER (
    ORDER BY date_column
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
```

**30-Day Rolling Average:**
```sql
AVG(column) OVER (
    ORDER BY date_column
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
)
```

### Use Cases:

| Pattern | Real-world | Example |
|---------|-----------|---------|
| Cumulative Sum | Revenue tracking | Total revenue to date |
| Cumulative Sum | Subscriber growth | Total users ever |
| Moving Average | Smoothing trends | 7-day engagement |
| Moving Average | Anomaly detection | Spot spikes |
| Moving Average | Growth tracking | Month-over-month |

### Things to Remember:

✓ ORDER BY determines "time direction" (always use date column)
✓ PARTITION BY for per-entity calculations (per user, per channel, etc.)
✓ Frame clause matters: UNBOUNDED PRECEDING = all history
✓ ROUND() makes output readable
✓ Use CTE to compute daily aggregates first, then window functions

### Common Mistakes:

❌ Forgetting ROWS BETWEEN (default might not be what you want)
❌ Wrong time period (29 for 30-day, 6 for 7-day)
❌ ORDER BY wrong column (must be date/time)
❌ Not using CTE when combining GROUP BY + window functions
❌ Forgetting PARTITION BY (causes global calculation instead of per-entity)

### Performance Tips:

- Window functions are generally fast
- ORDER BY a date column (indexed if possible)
- PARTITION BY reduces calculation scope (faster)
- CTE with GROUP BY, then window function (often faster than nested windows)

