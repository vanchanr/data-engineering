# Pattern 1: Aggregation with GROUP BY - Solutions

## Solution 1: Twitter - Daily Tweet Activity Metrics

**Problem**: Find daily tweet activity with aggregated metrics for days with 100+ tweets.

```sql
SELECT 
    DATE(created_at) as date,
    COUNT(*) as total_tweets,
    SUM(likes_count) as total_likes,
    AVG(retweets_count) as avg_retweets_per_tweet
FROM tweets
GROUP BY DATE(created_at)
HAVING COUNT(*) > 100
ORDER BY date DESC;
```

**Explanation**:
- `DATE(created_at)` converts timestamp to date and groups by day
- `COUNT(*)` counts all tweets that day
- `SUM(likes_count)` totals all likes across tweets
- `AVG(retweets_count)` calculates average retweets per tweet
- `HAVING COUNT(*) > 100` filters groups after aggregation (only days with 100+ tweets)
- This query is used for Twitter's analytics dashboard showing daily engagement trends

**Key Learning**: GROUP BY aggregates rows, HAVING filters the aggregated groups (different from WHERE which filters rows before aggregation).

---

## Solution 2: YouTube - Channel Performance Ranking

**Problem**: Calculate multi-metric channel performance and rank by watch hours.

```sql
SELECT 
    c.channel_id,
    c.channel_name,
    COUNT(v.video_id) as total_videos,
    SUM(v.views) as total_views,
    SUM(v.watch_time_hours) as total_watch_hours,
    AVG(v.watch_time_hours) as avg_watch_hours_per_video,
    ROW_NUMBER() OVER (ORDER BY SUM(v.watch_time_hours) DESC) as watch_hours_rank
FROM channels c
LEFT JOIN videos v ON c.channel_id = v.channel_id
GROUP BY c.channel_id, c.channel_name
ORDER BY total_watch_hours DESC
LIMIT 10;
```

**Explanation**:
- LEFT JOIN includes channels even if they have no videos (using COUNT will be 0)
- GROUP BY is on both channel_id and channel_name (required by SQL standard)
- `SUM(v.watch_time_hours)` aggregates watch hours across all videos per channel
- Window function `ROW_NUMBER() OVER (ORDER BY...)` ranks channels without grouping
- LIMIT 10 returns only top 10 channels
- Used for content discovery and channel analytics

**Key Learning**: When grouping, all non-aggregated columns must be in GROUP BY. Window functions can be added to GROUP BY queries for additional ranking/analysis.

---

## Solution 3: Spotify - Artist Revenue Breakdown by Country

**Problem**: Calculate artist performance metrics by country with filtering.

```sql
SELECT 
    a.artist_id,
    a.artist_name,
    a.genre,
    s.country,
    COUNT(*) as total_streams,
    SUM(s.revenue_generated) as total_revenue,
    AVG(s.revenue_generated) as avg_revenue_per_stream
FROM artists a
INNER JOIN streams s ON a.artist_id = s.artist_id
WHERE s.stream_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY a.artist_id, a.artist_name, a.genre, s.country
HAVING COUNT(*) > 10000
ORDER BY a.artist_name, total_revenue DESC;
```

**Explanation**:
- INNER JOIN ensures only artists with streams are included
- WHERE filters streams to last 90 days before aggregation
- GROUP BY on 4 dimensions (artist, genre, country) for granular metrics
- HAVING COUNT(*) > 10000 filters combinations with enough volume (data quality)
- Multiple aggregates in single query (COUNT, SUM, AVG) for efficiency
- Real use case: Spotify payment calculations and artist compensation

**Key Learning**: WHERE filters raw data, HAVING filters aggregated results. Use multiple aggregates for efficiency rather than separate queries.

---

## Solution 4: Instagram - Hashtag Performance Analysis

**Problem**: Analyze hashtag effectiveness with multi-metric engagement calculation.

```sql
SELECT 
    h.hashtag_id,
    h.hashtag_name,
    COUNT(DISTINCT ph.post_id) as post_count,
    AVG(p.likes) as avg_likes,
    AVG(p.comments) as avg_comments,
    AVG(p.shares) as avg_shares,
    SUM(p.likes + p.comments + p.shares) as total_engagement
FROM hashtags h
INNER JOIN post_hashtags ph ON h.hashtag_id = ph.hashtag_id
INNER JOIN posts p ON ph.post_id = p.post_id
WHERE ph.hashtag_id IN (
    SELECT hashtag_id 
    FROM post_hashtags 
    GROUP BY hashtag_id 
    HAVING COUNT(*) > 500
)
GROUP BY h.hashtag_id, h.hashtag_name
ORDER BY total_engagement DESC
LIMIT 20;
```

**Explanation**:
- Using `COUNT(DISTINCT ph.post_id)` counts unique posts (hashtag can appear once per post)
- Multiple averages show engagement per post (not per hashtag usage)
- Total engagement sums across all posts using that hashtag
- Subquery pre-filters hashtags used 500+ times for performance
- Demonstrates joining multiple tables with aggregation
- Real use case: Trending hashtag analysis and content recommendation

**Key Learning**: Use DISTINCT in COUNT when entities can appear multiple times. Subqueries can pre-filter large datasets for efficiency.

---

## Solution 5: Amazon - Product Category Sales Summary

**Problem**: Multi-level aggregation for sales metrics by category and subcategory.

```sql
SELECT 
    p.category_id,
    p.subcategory_id,
    COUNT(DISTINCT p.product_id) as product_count,
    SUM(o.quantity * p.price * (1 - o.discount_percentage/100)) as total_revenue,
    AVG(o.quantity * p.price * (1 - o.discount_percentage/100)) as avg_order_value,
    COUNT(DISTINCT o.user_id) as distinct_customers
FROM products p
INNER JOIN orders o ON p.product_id = o.product_id
WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
GROUP BY p.category_id, p.subcategory_id
HAVING SUM(o.quantity * p.price * (1 - o.discount_percentage/100)) > 100000
ORDER BY total_revenue DESC;
```

**Explanation**:
- Revenue calculation: quantity × price × (1 - discount%) 
- Used twice in query (for SUM and AVG) - in production, use derived columns or CTEs
- COUNT(DISTINCT p.product_id) counts unique products in each category combination
- COUNT(DISTINCT o.user_id) counts unique customers (customer lifetime value metric)
- HAVING filters after aggregation for business threshold ($100k+ revenue)
- Real use case: Marketplace category performance and resource allocation

**Key Learning**: Complex calculations can be embedded in aggregates. DISTINCT is used when counting unique entities across duplicated rows.

---

## Solution 6: Netflix - User Engagement by Device Type

**Problem**: Segment engagement metrics by device and subscription tier.

```sql
SELECT 
    wh.device_type,
    u.subscription_tier,
    COUNT(DISTINCT wh.user_id) as active_users,
    SUM(wh.duration_minutes) / 60.0 as total_watch_hours,
    AVG(wh.duration_minutes) as avg_watch_duration,
    SUM(CASE WHEN wh.completed = TRUE THEN 1 ELSE 0 END) / 
    COUNT(*) as completion_rate
FROM watch_history wh
INNER JOIN users u ON wh.user_id = u.user_id
WHERE wh.watch_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY wh.device_type, u.subscription_tier
HAVING COUNT(DISTINCT wh.user_id) >= 100
ORDER BY total_watch_hours DESC;
```

**Explanation**:
- `COUNT(DISTINCT wh.user_id)` counts unique users (multiple watches per user)
- `SUM(duration_minutes) / 60.0` converts minutes to hours
- `AVG(wh.duration_minutes)` is average per watch event (not per user)
- Completion rate uses CASE WHEN: true completions / total watches
- HAVING filters for minimum sample size (data quality requirement)
- Real use case: Device strategy and tier performance analysis

**Key Learning**: Use DISTINCT for unique entity counting. CASE WHEN enables ratio calculations within aggregates.

---

## Solution 7: Uber - Driver Performance Metrics

**Problem**: Multi-metric driver analytics with business filtering.

```sql
SELECT 
    d.driver_id,
    d.driver_name,
    d.vehicle_type,
    COUNT(r.ride_id) as total_rides,
    AVG(r.ride_time_minutes) as avg_duration_minutes,
    AVG(r.distance_km) as avg_distance_km,
    SUM(r.fare_amount) as total_earnings,
    AVG(r.rating) as avg_rating
FROM drivers d
INNER JOIN rides r ON d.driver_id = r.driver_id
WHERE r.ride_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    AND d.signup_date <= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY d.driver_id, d.driver_name, d.vehicle_type
HAVING COUNT(r.ride_id) >= 50
ORDER BY avg_rating DESC, total_earnings DESC;
```

**Explanation**:
- WHERE filters both ride data (last 90 days) and driver experience (90+ days tenure)
- Multiple averages show efficiency metrics (time, distance, revenue)
- SUM(fare_amount) shows total earnings (KPI for driver incentives)
- HAVING COUNT >= 50 ensures minimum activity for fair comparison
- ORDER BY shows business priority: quality (rating) first, then revenue
- Real use case: Driver performance-based incentives and quality metrics

**Key Learning**: WHERE can filter on multiple table conditions. Business logic determines both aggregation and sorting strategy.

---

## Solution 8: Google Drive - Storage Usage by File Type and User

**Problem**: Granular storage analytics with plan-aware segmentation.

```sql
SELECT 
    f.user_id,
    u.user_email,
    u.plan_type,
    f.file_type,
    COUNT(*) as file_count,
    SUM(f.size_bytes) / (1024*1024*1024) as total_storage_gb,
    AVG(f.size_bytes) / (1024*1024) as avg_file_size_mb,
    AVG(DATEDIFF(CURDATE(), f.last_accessed_date)) as avg_days_since_access
FROM files f
INNER JOIN users u ON f.user_id = u.user_id
GROUP BY f.user_id, u.user_email, u.plan_type, f.file_type
HAVING SUM(f.size_bytes) / (1024*1024*1024) > 1
ORDER BY total_storage_gb DESC;
```

**Explanation**:
- Byte-to-GB conversion: / (1024*1024*1024) for display readability
- Byte-to-MB conversion: / (1024*1024) for average file size
- `DATEDIFF(CURDATE(), last_accessed_date)` calculates days since access
- HAVING filter: only storage combinations > 1GB (eliminates trivial usage)
- GROUP BY 4 dimensions: user, email, plan, file type for granular analysis
- Real use case: Storage quota management and inactive file detection

**Key Learning**: Unit conversions in aggregates improve readability. DATEDIFF enables time-based metrics.

---

## Solution 9: WhatsApp - Message Volume by Chat Type and Hour

**Problem**: Time-series aggregation with temporal grouping.

```sql
SELECT 
    HOUR(sent_timestamp) as hour_of_day,
    c.chat_type,
    COUNT(*) as total_messages,
    SUM(CASE WHEN m.has_media = TRUE THEN 1 ELSE 0 END) as messages_with_media,
    AVG(m.message_length) as avg_message_length
FROM messages m
INNER JOIN chats c ON m.chat_id = c.chat_id
WHERE m.sent_timestamp >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
GROUP BY HOUR(sent_timestamp), c.chat_type
HAVING COUNT(*) > 1000
ORDER BY sent_timestamp DESC, total_messages DESC;
```

**Explanation**:
- `HOUR(sent_timestamp)` extracts hour from timestamp for temporal grouping
- CASE WHEN for conditional counting (messages with media)
- GROUP BY temporal dimension (hour) and categorical dimension (chat type)
- HAVING filters for significant volume (1000+ messages per hour-type combo)
- ORDER BY shows temporal ordering, then volume
- Real use case: Real-time traffic analysis, capacity planning, peak load identification

**Key Learning**: Temporal functions (HOUR, DATE, MONTH) enable time-series aggregation. Conditional counts identify message characteristics.

---

## Solution 10: LinkedIn - Skills Endorsement Distribution

**Problem**: Temporal aggregation with growth rate calculation.

```sql
SELECT 
    s.skill_id,
    s.skill_name,
    COUNT(e.endorsement_id) as total_endorsements,
    COUNT(DISTINCT e.user_id) as users_endorsed,
    COUNT(e.endorsement_id) / NULLIF(COUNT(DISTINCT e.user_id), 0) as avg_endorsements_per_user,
    (
        SUM(CASE WHEN DATE_FORMAT(e.endorsed_date, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m') THEN 1 ELSE 0 END) -
        SUM(CASE WHEN DATE_FORMAT(e.endorsed_date, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m') THEN 1 ELSE 0 END)
    ) / 
    NULLIF(SUM(CASE WHEN DATE_FORMAT(e.endorsed_date, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m') THEN 1 ELSE 0 END), 0) * 100 as endorsement_growth_percentage
FROM endorsements e
INNER JOIN skills s ON e.skill_id = s.skill_id
WHERE s.category = 'Software Development'
    AND e.endorsed_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY s.skill_id, s.skill_name
HAVING COUNT(e.endorsement_id) >= 100
ORDER BY total_endorsements DESC;
```

**Explanation**:
- `COUNT(DISTINCT e.user_id)` counts unique users endorsed with this skill (can be endorsed multiple times)
- `NULLIF(..., 0)` prevents division by zero in growth rate calculation
- Growth rate uses CASE WHEN with DATE_FORMAT to partition by month
- Current month count - previous month count / previous month count * 100
- DATE_FORMAT extracts year-month for month-level grouping
- Real use case: Skill marketplace analytics, identifying trending vs declining skills

**Key Learning**: NULLIF prevents division by zero errors. Complex calculations can be embedded in SELECT for derived metrics. CASE WHEN enables conditional aggregation within ratios.

---

## Performance Considerations

1. **Indexing**: GROUP BY columns should be indexed for large tables (created_at, user_id, product_id, etc.)
2. **Filter Early**: Use WHERE before GROUP BY to reduce aggregation scope
3. **DISTINCT Overhead**: COUNT(DISTINCT ...) is slower than alternatives; consider pre-filtering
4. **Large Aggregations**: For very large datasets, consider materialized views or partitioning
5. **Multi-level Grouping**: 4+ GROUP BY columns can be expensive; consider staging tables

## Common Patterns to Internalize

1. **Multi-metric queries**: Combine COUNT, SUM, AVG, MIN, MAX in single query
2. **Filtering aggregates**: Use HAVING not WHERE for aggregate filters
3. **Temporal grouping**: Use DATE(), MONTH(), HOUR() functions for time series
4. **Unique entity counting**: Use COUNT(DISTINCT column)
5. **Conditional aggregates**: Use CASE WHEN within aggregate functions for segmentation
