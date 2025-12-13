# Pattern 3: Ranking with Window Functions - Solutions

## Solution 1: YouTube - Top N Videos Per Creator

**Problem**: Find top 3 most-watched videos for each creator.

```sql
SELECT 
    c.channel_id,
    c.channel_name,
    v.video_id,
    v.title as video_title,
    v.views,
    v.watch_time_hours,
    ROW_NUMBER() OVER (PARTITION BY c.channel_id ORDER BY v.views DESC) as video_rank
FROM channels c
INNER JOIN videos v ON c.channel_id = v.channel_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.channel_id ORDER BY v.views DESC) <= 3
ORDER BY c.channel_name, video_rank;
```

**Explanation**:
- `PARTITION BY channel_id`: Restart ranking for each creator
- `ORDER BY views DESC`: Rank by view count (highest first)
- `ROW_NUMBER()`: Gives unique sequential numbers (1, 2, 3, 4...) even with ties
- `QUALIFY` clause filters window function results (more efficient than WHERE)
- Real use case: Creator analytics dashboard, content discovery

**Alternative with CTE** (for databases without QUALIFY):
```sql
WITH ranked_videos AS (
    SELECT 
        c.channel_id,
        c.channel_name,
        v.video_id,
        v.title as video_title,
        v.views,
        v.watch_time_hours,
        ROW_NUMBER() OVER (PARTITION BY c.channel_id ORDER BY v.views DESC) as video_rank
    FROM channels c
    INNER JOIN videos v ON c.channel_id = v.channel_id
)
SELECT *
FROM ranked_videos
WHERE video_rank <= 3
ORDER BY channel_name, video_rank;
```

**Key Learning**: ROW_NUMBER assigns unique ranks even for tied values. PARTITION BY resets ranking for each group.

---

## Solution 2: Amazon - Product Price Quartiles

**Problem**: Divide products into 4 price tiers (quartiles) within each category.

```sql
SELECT 
    p.category_id,
    c.category_name,
    p.product_id,
    p.product_name,
    p.price,
    NTILE(4) OVER (PARTITION BY p.category_id ORDER BY p.price ASC) as price_quartile
FROM products p
INNER JOIN categories c ON p.category_id = c.category_id
WHERE NTILE(4) OVER (PARTITION BY p.category_id ORDER BY p.price ASC) IN (1, 4)
ORDER BY p.category_id, price_quartile, p.price;
```

**Explanation**:
- `NTILE(4)`: Divides products into 4 equal buckets within each category
- `PARTITION BY category_id`: Separate quartile calculation per category
- `ORDER BY price ASC`: Orders by price to create quartile split
- Quartile 1 (Q1) = cheapest 25%, Quartile 4 (Q4) = most expensive 25%
- Real use case: Price positioning analysis, market segmentation

**More explicit version showing all quartiles**:
```sql
SELECT 
    p.category_id,
    c.category_name,
    p.product_id,
    p.product_name,
    p.price,
    NTILE(4) OVER (PARTITION BY p.category_id ORDER BY p.price ASC) as price_quartile,
    CASE 
        WHEN NTILE(4) OVER (PARTITION BY p.category_id ORDER BY p.price ASC) = 1 THEN 'Budget'
        WHEN NTILE(4) OVER (PARTITION BY p.category_id ORDER BY p.price ASC) = 2 THEN 'Standard'
        WHEN NTILE(4) OVER (PARTITION BY p.category_id ORDER BY p.price ASC) = 3 THEN 'Premium'
        ELSE 'Ultra-Premium'
    END as price_tier
FROM products p
INNER JOIN categories c ON p.category_id = c.category_id
ORDER BY p.category_id, price_quartile, p.price;
```

**Key Learning**: NTILE divides data into N buckets. Use CASE WHEN for business-friendly tier names.

---

## Solution 3: LinkedIn - Job Title Career Progression

**Problem**: Track career progression by showing previous, current, and next job.

```sql
SELECT 
    u.user_id,
    u.user_name,
    jh.start_date,
    LAG(jh.job_title) OVER (PARTITION BY u.user_id ORDER BY jh.start_date) as previous_job_title,
    jh.job_title as current_job_title,
    jh.company_name as current_company,
    LEAD(jh.job_title) OVER (PARTITION BY u.user_id ORDER BY jh.start_date) as next_job_title,
    LEAD(jh.start_date) OVER (PARTITION BY u.user_id ORDER BY jh.start_date) as next_start_date
FROM users u
INNER JOIN job_history jh ON u.user_id = jh.user_id
ORDER BY u.user_id, jh.start_date DESC;
```

**Explanation**:
- `LAG()` retrieves previous row's job title (one row back in time)
- `LEAD()` retrieves next row's job title (one row forward in time)
- `PARTITION BY user_id`: Each user's progression tracked separately
- `ORDER BY start_date`: Temporal ordering for career timeline
- Returns NULL for first (no LAG) and last (no LEAD) jobs
- Real use case: Career trajectory analysis, talent development

**With salary comparisons** (if available):
```sql
SELECT 
    u.user_id,
    u.user_name,
    jh.start_date,
    LAG(jh.job_title) OVER (PARTITION BY u.user_id ORDER BY jh.start_date) as previous_job_title,
    jh.job_title as current_job_title,
    jh.company_name as current_company,
    LEAD(jh.job_title) OVER (PARTITION BY u.user_id ORDER BY jh.start_date) as next_job_title,
    CASE 
        WHEN jh.salary > LAG(jh.salary) OVER (PARTITION BY u.user_id ORDER BY jh.start_date)
            THEN 'Promotion'
        WHEN jh.salary < LAG(jh.salary) OVER (PARTITION BY u.user_id ORDER BY jh.start_date)
            THEN 'Lateral'
        ELSE 'Same'
    END as career_movement
FROM users u
INNER JOIN job_history jh ON u.user_id = jh.user_id
ORDER BY u.user_id, jh.start_date;
```

**Key Learning**: LAG/LEAD enable row-to-row comparisons without self-joins. Useful for temporal analysis and trend detection.

---

## Solution 4: Instagram - Follower Growth Ranking by Day

**Problem**: Calculate daily follower growth and rank accounts by growth rate.

```sql
SELECT 
    u.user_id,
    u.user_name,
    dfs.date,
    dfs.follower_count,
    dfs.follower_count - LAG(dfs.follower_count) OVER (
        PARTITION BY u.user_id 
        ORDER BY dfs.date
    ) as daily_growth,
    RANK() OVER (
        PARTITION BY DATE(dfs.date)
        ORDER BY (dfs.follower_count - LAG(dfs.follower_count) OVER (
            PARTITION BY u.user_id 
            ORDER BY dfs.date
        )) DESC
    ) as daily_growth_rank
FROM users u
INNER JOIN daily_follower_stats dfs ON u.user_id = dfs.user_id
WHERE u.account_type = 'influencer'
    AND dfs.date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
QUALIFY RANK() OVER (
    PARTITION BY DATE(dfs.date)
    ORDER BY (dfs.follower_count - LAG(dfs.follower_count) OVER (
        PARTITION BY u.user_id 
        ORDER BY dfs.date
    )) DESC
) <= 10
ORDER BY dfs.date DESC, daily_growth_rank;
```

**Explanation**:
- Inner LAG calculates daily growth (today's followers - yesterday's)
- Outer RANK windows create daily ranking across all influencers
- `PARTITION BY DATE(date)`: Separate ranking for each day
- `ORDER BY daily_growth DESC`: Rank by growth amount
- QUALIFY filters to top 10 daily growers
- Real use case: Viral content detection, growth hacking analytics

**Simpler version**:
```sql
WITH daily_growth_calc AS (
    SELECT 
        u.user_id,
        u.user_name,
        dfs.date,
        dfs.follower_count,
        dfs.follower_count - LAG(dfs.follower_count) OVER (
            PARTITION BY u.user_id 
            ORDER BY dfs.date
        ) as daily_growth
    FROM users u
    INNER JOIN daily_follower_stats dfs ON u.user_id = dfs.user_id
    WHERE u.account_type = 'influencer'
        AND dfs.date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
)
SELECT 
    user_id,
    user_name,
    date,
    follower_count,
    daily_growth,
    RANK() OVER (
        PARTITION BY date
        ORDER BY daily_growth DESC
    ) as daily_growth_rank
FROM daily_growth_calc
WHERE daily_growth IS NOT NULL
ORDER BY date DESC, daily_growth_rank;
```

**Key Learning**: LAG/LEAD enable growth calculations. Multiple PARTITION BY create multi-dimensional rankings.

---

## Solution 5: Spotify - Listener Percentile by Streams

**Problem**: Compare RANK vs DENSE_RANK for tied artist streams.

```sql
SELECT 
    a.artist_id,
    a.artist_name,
    s.genre,
    s.month,
    s.total_streams,
    RANK() OVER (
        PARTITION BY s.genre 
        ORDER BY s.total_streams DESC
    ) as rank_position,
    DENSE_RANK() OVER (
        PARTITION BY s.genre 
        ORDER BY s.total_streams DESC
    ) as dense_rank_position,
    CASE 
        WHEN RANK() OVER (
            PARTITION BY s.genre 
            ORDER BY s.total_streams DESC
        ) != DENSE_RANK() OVER (
            PARTITION BY s.genre 
            ORDER BY s.total_streams DESC
        ) THEN TRUE
        ELSE FALSE
    END as has_tie,
    RANK() OVER (
        PARTITION BY s.genre 
        ORDER BY s.total_streams DESC
    ) - DENSE_RANK() OVER (
        PARTITION BY s.genre 
        ORDER BY s.total_streams DESC
    ) as rank_difference
FROM artists a
INNER JOIN artist_stats s ON a.artist_id = s.artist_id
ORDER BY s.genre, rank_position, a.artist_name;
```

**Explanation**:
- `RANK()`: Assigns same rank to tied values, skips numbers (1, 1, 3, 4)
- `DENSE_RANK()`: Assigns same rank to tied values, continuous (1, 1, 2, 3)
- Difference appears only when multiple artists have identical streams
- `has_tie` identifies positions where ties occur
- `rank_difference` shows how many positions were skipped
- Real use case: Artist performance tiers, tie-breaking logic

**Real-world scenario**:
```
3 artists with 1,000,000 streams each:
- RANK: All get rank 1, next artist gets rank 4
- DENSE_RANK: All get rank 1, next artist gets rank 2

Use RANK when you need gap (e.g., "top 3 positions")
Use DENSE_RANK when you need continuous numbering
```

**Key Learning**: RANK handles ties with gaps, DENSE_RANK without. Choose based on business logic.

---

## Solution 6: Netflix - Watch Pattern First and Last Content

**Problem**: Track first and last content watched using window functions.

```sql
SELECT 
    wh.user_id,
    FIRST_VALUE(c.content_id) OVER (
        PARTITION BY wh.user_id 
        ORDER BY wh.watched_date ASC
    ) as first_content_watched,
    MIN(wh.watched_date) OVER (
        PARTITION BY wh.user_id
    ) as first_watch_date,
    LAST_VALUE(c.content_id) OVER (
        PARTITION BY wh.user_id 
        ORDER BY wh.watched_date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as last_content_watched,
    MAX(wh.watched_date) OVER (
        PARTITION BY wh.user_id
    ) as last_watch_date,
    SUM(wh.duration_minutes) / 60.0 as total_watch_hours,
    ROUND(
        (FIRST_VALUE(wh.duration_minutes) OVER (
            PARTITION BY wh.user_id 
            ORDER BY wh.watched_date ASC
        ) / SUM(wh.duration_minutes) OVER (
            PARTITION BY wh.user_id
        )) * 100, 2
    ) as first_content_percentage
FROM watch_history wh
INNER JOIN content c ON wh.content_id = c.content_id
WHERE wh.watched_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY wh.user_id ORDER BY wh.watched_date) = 1
ORDER BY wh.user_id;
```

**Explanation**:
- `FIRST_VALUE()`: Gets first row's value in partition
- `LAST_VALUE()`: Gets last row's value (requires frame specification)
- `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`: Required for LAST_VALUE to see all rows
- Without frame, LAST_VALUE sees only up to current row (not useful here)
- Real use case: Onboarding analysis, content discovery patterns

**Simpler approach**:
```sql
WITH user_watch_stats AS (
    SELECT 
        user_id,
        SUM(duration_minutes) as total_watch_minutes,
        MIN(watched_date) as first_watch_date,
        MAX(watched_date) as last_watch_date
    FROM watch_history
    WHERE watched_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY user_id
),
first_and_last AS (
    SELECT 
        user_id,
        content_id,
        watched_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY watched_date ASC) as watch_order,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY watched_date DESC) as watch_order_desc
    FROM watch_history
    WHERE watched_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
)
SELECT 
    uws.user_id,
    MAX(CASE WHEN fal.watch_order = 1 THEN fal.content_id END) as first_content,
    uws.first_watch_date,
    MAX(CASE WHEN fal.watch_order_desc = 1 THEN fal.content_id END) as last_content,
    uws.last_watch_date,
    uws.total_watch_minutes / 60.0 as total_watch_hours
FROM user_watch_stats uws
LEFT JOIN first_and_last fal ON uws.user_id = fal.user_id
GROUP BY uws.user_id, uws.first_watch_date, uws.last_watch_date, uws.total_watch_minutes
ORDER BY uws.user_id;
```

**Key Learning**: FIRST_VALUE/LAST_VALUE need explicit frame specification. Row-based approach is often clearer.

---

## Solution 7: Uber - Driver Earnings Week-over-Week

**Problem**: Calculate driver week-over-week earnings change and rank.

```sql
WITH weekly_earnings AS (
    SELECT 
        d.driver_id,
        d.driver_name,
        DATE_TRUNC(de.date, WEEK) as week_start,
        SUM(de.earnings) as week_earnings
    FROM drivers d
    INNER JOIN daily_earnings de ON d.driver_id = de.driver_id
    WHERE de.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY d.driver_id, d.driver_name, DATE_TRUNC(de.date, WEEK)
)
SELECT 
    driver_id,
    driver_name,
    week_earnings as current_week_earnings,
    LAG(week_earnings) OVER (
        PARTITION BY driver_id 
        ORDER BY week_start
    ) as previous_week_earnings,
    ROUND(
        (week_earnings - LAG(week_earnings) OVER (
            PARTITION BY driver_id 
            ORDER BY week_start
        )) / LAG(week_earnings) OVER (
            PARTITION BY driver_id 
            ORDER BY week_start
        ) * 100, 2
    ) as wow_change_percentage,
    RANK() OVER (
        ORDER BY (week_earnings - LAG(week_earnings) OVER (
            PARTITION BY driver_id 
            ORDER BY week_start
        )) / LAG(week_earnings) OVER (
            PARTITION BY driver_id 
            ORDER BY week_start
        ) DESC
    ) as growth_rank
FROM weekly_earnings
WHERE week_earnings > LAG(week_earnings) OVER (
    PARTITION BY driver_id 
    ORDER BY week_start
)
ORDER BY growth_rank
LIMIT 20;
```

**Explanation**:
- CTE aggregates earnings by week
- LAG gets previous week's earnings for comparison
- Growth rate calculation: (current - previous) / previous * 100
- RANK across all drivers by growth rate (highest first)
- WHERE filters to only positive growth drivers
- LIMIT 20 shows top 20 growing drivers
- Real use case: Driver incentive program, top performer identification

**With null handling**:
```sql
WITH weekly_earnings AS (
    SELECT 
        d.driver_id,
        d.driver_name,
        DATE_TRUNC(de.date, WEEK) as week_start,
        SUM(de.earnings) as week_earnings
    FROM drivers d
    INNER JOIN daily_earnings de ON d.driver_id = de.driver_id
    WHERE de.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY d.driver_id, d.driver_name, DATE_TRUNC(de.date, WEEK)
),
earnings_with_lag AS (
    SELECT 
        driver_id,
        driver_name,
        week_start,
        week_earnings,
        LAG(week_earnings) OVER (PARTITION BY driver_id ORDER BY week_start) as prev_week,
        CASE 
            WHEN LAG(week_earnings) OVER (PARTITION BY driver_id ORDER BY week_start) IS NULL THEN NULL
            WHEN LAG(week_earnings) OVER (PARTITION BY driver_id ORDER BY week_start) = 0 THEN NULL
            ELSE (week_earnings - LAG(week_earnings) OVER (PARTITION BY driver_id ORDER BY week_start)) / 
                 LAG(week_earnings) OVER (PARTITION BY driver_id ORDER BY week_start) * 100
        END as wow_change_percentage
    FROM weekly_earnings
)
SELECT *
FROM earnings_with_lag
WHERE wow_change_percentage IS NOT NULL
ORDER BY wow_change_percentage DESC
LIMIT 20;
```

**Key Learning**: LAG for comparisons, NULL handling for division by zero. RANK for multi-driver comparison.

---

## Solution 8: Google Drive - Storage Growth Tracking

**Problem**: Track monthly storage growth with acceleration detection.

```sql
WITH monthly_growth AS (
    SELECT 
        u.user_id,
        u.user_name,
        u.plan_type,
        mss.snapshot_month,
        mss.storage_used_bytes / (1024*1024*1024) as storage_gb,
        LAG(mss.storage_used_bytes / (1024*1024*1024)) OVER (
            PARTITION BY u.user_id 
            ORDER BY mss.snapshot_month
        ) as prev_month_storage_gb,
        ROUND(
            (mss.storage_used_bytes - LAG(mss.storage_used_bytes) OVER (
                PARTITION BY u.user_id 
                ORDER BY mss.snapshot_month
            )) / LAG(mss.storage_used_bytes) OVER (
                PARTITION BY u.user_id 
                ORDER BY mss.snapshot_month
            ) * 100, 2
        ) as mom_growth_percent,
        SUM(mss.storage_used_bytes / (1024*1024*1024)) OVER (
            PARTITION BY u.user_id 
            ORDER BY mss.snapshot_month
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as cumulative_6month_growth_gb
    FROM users u
    INNER JOIN monthly_storage_snapshots mss ON u.user_id = mss.user_id
    WHERE u.plan_type LIKE '%100GB%' OR u.plan_type LIKE '%Premium%'
)
SELECT 
    user_id,
    user_name,
    plan_type,
    snapshot_month,
    storage_gb,
    prev_month_storage_gb,
    mom_growth_percent,
    cumulative_6month_growth_gb,
    ROUND(storage_gb / 100 * 100, 2) as percent_of_limit_used,
    CASE 
        WHEN ROUND(storage_gb / 100 * 100, 2) > 80 THEN TRUE
        ELSE FALSE
    END as approaching_limit
FROM monthly_growth
WHERE storage_gb > 1
ORDER BY user_id, snapshot_month DESC;
```

**Explanation**:
- LAG calculates month-over-month change
- SUM with ROWS BETWEEN calculates cumulative growth
- Percent of limit assumes 100GB plan (adjust for other plans)
- Approaching limit logic: >80% of quota
- Real use case: Storage quota management, churn prediction (running out of space = churn risk)

**Acceleration detection**:
```sql
WITH growth_rates AS (
    SELECT 
        user_id,
        snapshot_month,
        storage_gb,
        LAG(storage_gb) OVER (PARTITION BY user_id ORDER BY snapshot_month) as prev_storage,
        storage_gb - LAG(storage_gb) OVER (PARTITION BY user_id ORDER BY snapshot_month) as monthly_growth,
        LAG(storage_gb - LAG(storage_gb) OVER (PARTITION BY user_id ORDER BY snapshot_month)) OVER (
            PARTITION BY user_id ORDER BY snapshot_month
        ) as prev_monthly_growth
    FROM (
        SELECT 
            user_id,
            snapshot_month,
            storage_used_bytes / (1024*1024*1024) as storage_gb
        FROM monthly_storage_snapshots
    )
)
SELECT 
    user_id,
    snapshot_month,
    storage_gb,
    monthly_growth,
    CASE 
        WHEN monthly_growth > prev_monthly_growth THEN 'Accelerating'
        WHEN monthly_growth < prev_monthly_growth THEN 'Decelerating'
        ELSE 'Stable'
    END as growth_trend
FROM growth_rates
WHERE monthly_growth IS NOT NULL AND prev_monthly_growth IS NOT NULL
ORDER BY user_id, snapshot_month DESC;
```

**Key Learning**: LAG for comparisons, SUM OVER for cumulative totals, ROWS BETWEEN for rolling calculations.

---

## Solution 9: Twitter - Hashtag Ranking Dynamics

**Problem**: Track hashtag rank changes across hours and identify volatility.

```sql
WITH hourly_ranks AS (
    SELECT 
        h.hashtag_id,
        h.hashtag_name,
        hts.hour,
        hts.engagement_score,
        RANK() OVER (
            PARTITION BY hts.hour 
            ORDER BY hts.engagement_score DESC
        ) as hourly_rank
    FROM hashtags h
    INNER JOIN hourly_trend_stats hts ON h.hashtag_id = hts.hashtag_id
    WHERE hts.hour >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
)
SELECT 
    hashtag_id,
    hashtag_name,
    MIN(hourly_rank) as min_rank,
    MAX(hourly_rank) as max_rank,
    MAX(hourly_rank) - MIN(hourly_rank) as rank_volatility,
    SUM(CASE WHEN hourly_rank <= 10 THEN 1 ELSE 0 END) as appearances_in_top_10,
    COUNT(*) as trending_hours
FROM hourly_ranks
GROUP BY hashtag_id, hashtag_name
HAVING SUM(CASE WHEN hourly_rank <= 10 THEN 1 ELSE 0 END) > 0
ORDER BY rank_volatility DESC, min_rank ASC;
```

**Explanation**:
- RANK for each hour creates hourly leaderboards
- MIN/MAX identify rank range across 7 days
- Volatility = max_rank - min_rank (higher = more volatile)
- appearances_in_top_10 = consistency metric
- trending_hours = how long hashtag was trending
- Real use case: Trending topic analysis, viral detection

**Advanced with trend classification**:
```sql
WITH hourly_ranks AS (
    SELECT 
        h.hashtag_id,
        h.hashtag_name,
        hts.hour,
        hts.engagement_score,
        RANK() OVER (
            PARTITION BY hts.hour 
            ORDER BY hts.engagement_score DESC
        ) as hourly_rank,
        LAG(RANK() OVER (
            PARTITION BY hts.hour 
            ORDER BY hts.engagement_score DESC
        )) OVER (PARTITION BY h.hashtag_id ORDER BY hts.hour) as prev_hour_rank
    FROM hashtags h
    INNER JOIN hourly_trend_stats hts ON h.hashtag_id = hts.hashtag_id
    WHERE hts.hour >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
)
SELECT 
    hashtag_id,
    hashtag_name,
    MIN(hourly_rank) as current_position,
    MAX(hourly_rank) - MIN(hourly_rank) as volatility,
    SUM(CASE WHEN hourly_rank <= 10 THEN 1 ELSE 0 END) as top10_appearances,
    CASE 
        WHEN MAX(hourly_rank) - MIN(hourly_rank) > 50 THEN 'Highly Volatile'
        WHEN MAX(hourly_rank) - MIN(hourly_rank) > 20 THEN 'Volatile'
        WHEN MAX(hourly_rank) - MIN(hourly_rank) > 5 THEN 'Stable'
        ELSE 'Very Stable'
    END as volatility_category
FROM hourly_ranks
GROUP BY hashtag_id, hashtag_name
ORDER BY volatility DESC;
```

**Key Learning**: RANK within time windows for multi-period analysis. MAX/MIN identify range across periods.

---

## Solution 10: LinkedIn - Skill Endorsement Momentum

**Problem**: Track skills with accelerating demand using window functions.

```sql
WITH weekly_skills AS (
    SELECT 
        s.skill_id,
        s.skill_name,
        s.category,
        DATE_TRUNC(ss.week, WEEK) as week_start,
        SUM(ss.endorsement_count) as endorsements_this_week,
        COUNT(DISTINCT ss.unique_endorsers) as unique_endorsers_this_week
    FROM skills s
    INNER JOIN skill_stats ss ON s.skill_id = ss.skill_id
    WHERE ss.week >= DATE_SUB(CURDATE(), INTERVAL 12 WEEK)
    GROUP BY s.skill_id, s.skill_name, s.category, DATE_TRUNC(ss.week, WEEK)
),
ranked_and_lagged AS (
    SELECT 
        skill_id,
        skill_name,
        category,
        week_start,
        endorsements_this_week,
        RANK() OVER (
            PARTITION BY week_start 
            ORDER BY endorsements_this_week DESC
        ) as week_rank,
        LAG(endorsements_this_week) OVER (
            PARTITION BY skill_id 
            ORDER BY week_start
        ) as prev_week_endorsements,
        SUM(endorsements_this_week) OVER (
            PARTITION BY skill_id 
            ORDER BY week_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_endorsements,
        SUM(CASE 
            WHEN endorsements_this_week > LAG(endorsements_this_week) OVER (
                PARTITION BY skill_id 
                ORDER BY week_start
            ) THEN 1
            ELSE 0
        END) OVER (
            PARTITION BY skill_id 
            ORDER BY week_start
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as weeks_of_growth_in_3week_window
    FROM weekly_skills
)
SELECT 
    skill_id,
    skill_name,
    category,
    week_start,
    endorsements_this_week,
    week_rank,
    prev_week_endorsements,
    ROUND(
        (endorsements_this_week - prev_week_endorsements) / prev_week_endorsements * 100, 2
    ) as wow_change_percentage,
    cumulative_endorsements,
    weeks_of_growth_in_3week_window,
    CASE 
        WHEN weeks_of_growth_in_3week_window >= 2 THEN 'Accelerating'
        WHEN weeks_of_growth_in_3week_window = 1 THEN 'Growing'
        ELSE 'Declining'
    END as momentum_score
FROM ranked_and_lagged
WHERE endorsements_this_week >= 100
ORDER BY week_start DESC, momentum_score DESC, week_rank;
```

**Explanation**:
- CTE aggregates by week and skill
- RANK creates weekly rankings for context
- LAG tracks previous week's endorsements
- SUM OVER with UNBOUNDED calculates cumulative total
- SUM with ROWS BETWEEN counts growth weeks in 3-week window
- Acceleration detected by consecutive weeks of growth
- Real use case: Skill marketplace, talent demand trends

**Key Learning**: Window functions can be nested. ROWS BETWEEN enables rolling calculations. Multiple OVER clauses provide layered analysis.

---

## Window Function Performance Tips

1. **Frame specification**: Required for FIRST_VALUE/LAST_VALUE when looking beyond current row
2. **PARTITION BY**: Creates separate calculations for each group (critical for large datasets)
3. **ORDER BY**: Determines sort order within partition (affects RANK, LAG, LEAD results)
4. **QUALIFY clause**: More efficient than WHERE for filtering window function results
5. **Index strategy**: Partition columns should be indexed

## Common Patterns

1. **Top N per group**: ROW_NUMBER with PARTITION BY + QUALIFY
2. **Running totals**: SUM with ROWS BETWEEN UNBOUNDED PRECEDING
3. **Comparisons**: LAG/LEAD with PARTITION BY for temporal analysis
4. **Percentiles**: NTILE for dividing data into buckets
5. **Ranking variants**: Choose RANK, DENSE_RANK, or ROW_NUMBER based on tie handling needs
