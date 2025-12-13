# Pattern 4: Moving Averages & Cumulative Sums - Solutions

## Solution 1: Spotify - 7-Day Rolling Average Stream Count

**Problem**: Calculate 7-day rolling average streams with trend indication.

```sql
SELECT 
    a.artist_id,
    a.artist_name,
    ds.date,
    ds.stream_count as daily_streams,
    AVG(ds.stream_count) OVER (
        PARTITION BY a.artist_id 
        ORDER BY ds.date ASC
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as 7day_rolling_avg,
    CASE 
        WHEN AVG(ds.stream_count) OVER (
            PARTITION BY a.artist_id 
            ORDER BY ds.date ASC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) > AVG(ds.stream_count) OVER (
            PARTITION BY a.artist_id 
            ORDER BY ds.date ASC
            ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
        ) THEN 'Up'
        WHEN AVG(ds.stream_count) OVER (
            PARTITION BY a.artist_id 
            ORDER BY ds.date ASC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) < AVG(ds.stream_count) OVER (
            PARTITION BY a.artist_id 
            ORDER BY ds.date ASC
            ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
        ) THEN 'Down'
        ELSE 'Flat'
    END as trend_vs_previous_week
FROM artists a
LEFT JOIN daily_streams ds ON a.artist_id = ds.artist_id
WHERE ds.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
ORDER BY a.artist_name, ds.date ASC;
```

**Explanation**:
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`: 7-day window (current day + 6 previous days)
- First column AVG: Current 7-day average
- Second column AVG: Previous 7-day average (7-13 days ago)
- Comparison shows trend direction
- Real use case: Stream trend analysis, viral song detection

**Simpler approach without trend comparison**:
```sql
SELECT 
    a.artist_id,
    a.artist_name,
    ds.date,
    ds.stream_count,
    ROUND(
        AVG(ds.stream_count) OVER (
            PARTITION BY a.artist_id 
            ORDER BY ds.date ASC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) as 7day_rolling_avg
FROM artists a
INNER JOIN daily_streams ds ON a.artist_id = ds.artist_id
WHERE ds.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
ORDER BY a.artist_name, ds.date;
```

**Key Learning**: ROWS BETWEEN defines window size. BETWEEN 6 PRECEDING AND CURRENT ROW = 7 days. First few rows have <7 days (partial window).

---

## Solution 2: Netflix - 30-Day Cumulative Watch Hours for New Users

**Problem**: Track cumulative watch hours for first 30 days of subscription.

```sql
SELECT 
    u.user_id,
    wh.watched_date,
    DATEDIFF(wh.watched_date, u.signup_date) + 1 as day_of_subscription,
    ROUND(SUM(wh.duration_minutes) / 60.0, 2) as daily_watch_hours,
    ROUND(
        SUM(wh.duration_minutes) OVER (
            PARTITION BY u.user_id 
            ORDER BY wh.watched_date ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / 60.0, 2
    ) as cumulative_watch_hours,
    u.signup_date
FROM users u
INNER JOIN watch_history wh ON u.user_id = wh.user_id
WHERE DATEDIFF(wh.watched_date, u.signup_date) + 1 <= 30
    AND u.signup_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
GROUP BY u.user_id, wh.watched_date, u.signup_date
ORDER BY u.user_id, wh.watched_date;
```

**Explanation**:
- `DATEDIFF(watched_date, signup_date) + 1`: Day number of subscription (day 1 = signup day)
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`: Cumulative sum from start to current row
- GROUP BY aggregates by date (multiple watches per day summed)
- Real use case: Onboarding analysis, early engagement prediction

**With retention cohort analysis**:
```sql
WITH daily_cumulative AS (
    SELECT 
        u.user_id,
        wh.watched_date,
        DATEDIFF(wh.watched_date, u.signup_date) + 1 as day_of_subscription,
        SUM(wh.duration_minutes) / 60.0 as daily_watch_hours,
        SUM(SUM(wh.duration_minutes)) OVER (
            PARTITION BY u.user_id 
            ORDER BY wh.watched_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / 60.0 as cumulative_watch_hours,
        DATE_TRUNC(u.signup_date, MONTH) as signup_cohort
    FROM users u
    INNER JOIN watch_history wh ON u.user_id = wh.user_id
    WHERE DATEDIFF(wh.watched_date, u.signup_date) + 1 <= 30
    GROUP BY u.user_id, wh.watched_date, u.signup_date
)
SELECT 
    signup_cohort,
    day_of_subscription,
    COUNT(DISTINCT user_id) as active_users,
    ROUND(AVG(daily_watch_hours), 2) as avg_daily_hours,
    ROUND(AVG(cumulative_watch_hours), 2) as avg_cumulative_hours
FROM daily_cumulative
GROUP BY signup_cohort, day_of_subscription
ORDER BY signup_cohort, day_of_subscription;
```

**Key Learning**: UNBOUNDED PRECEDING creates true cumulative calculation. GROUP BY with window functions requires careful aggregation order.

---

## Solution 3: YouTube - 14-Day Moving Average with Gap Handling

**Problem**: Calculate 14-day moving average handling missing data days.

```sql
WITH video_with_dates AS (
    SELECT 
        v.video_id,
        v.title,
        vds.date,
        vds.views,
        COALESCE(vds.watch_time_hours, 0) as watch_time_hours,
        ROUND(
            AVG(COALESCE(vds.views, 0)) OVER (
                PARTITION BY v.video_id 
                ORDER BY vds.date ASC
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ), 2
        ) as 14day_moving_avg,
        CASE 
            WHEN COALESCE(vds.views, 0) > 2 * AVG(COALESCE(vds.views, 0)) OVER (
                PARTITION BY v.video_id 
                ORDER BY vds.date ASC
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) THEN TRUE
            ELSE FALSE
        END as is_anomaly_day,
        ROUND(
            COALESCE(vds.views, 0) / NULLIF(
                AVG(COALESCE(vds.views, 0)) OVER (
                    PARTITION BY v.video_id 
                    ORDER BY vds.date ASC
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ), 0
            ), 2
        ) as anomaly_factor
    FROM videos v
    LEFT JOIN video_daily_stats vds ON v.video_id = vds.video_id
    WHERE v.publish_date <= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        AND vds.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
)
SELECT 
    video_id,
    title,
    date,
    COALESCE(views, 0) as views,
    14day_moving_avg,
    is_anomaly_day,
    anomaly_factor
FROM video_with_dates
WHERE date IS NOT NULL
ORDER BY video_id, date;
```

**Explanation**:
- LEFT JOIN includes dates with no data (NULLs)
- COALESCE replaces NULL with 0 for calculation
- Moving average ignores NULL values automatically
- Anomaly detection: views > 2x moving average
- anomaly_factor = views / average (1.0 = average, 2.0 = double average)
- Real use case: Content performance anomaly detection

**With forward-fill for missing data**:
```sql
WITH date_spine AS (
    SELECT DATE_ADD(MIN(date), INTERVAL seq DAY) as date
    FROM video_daily_stats,
    (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 as seq 
     FROM ... LIMIT 90) seq
    WHERE DATE_ADD(MIN(date), INTERVAL seq DAY) <= CURDATE()
),
video_stats_filled AS (
    SELECT 
        v.video_id,
        v.title,
        ds.date,
        COALESCE(
            vds.views,
            LAST_VALUE(vds.views IGNORE NULLS) OVER (
                PARTITION BY v.video_id 
                ORDER BY ds.date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ) as filled_views
    FROM videos v
    CROSS JOIN date_spine ds
    LEFT JOIN video_daily_stats vds ON v.video_id = vds.video_id 
        AND ds.date = vds.date
    WHERE v.publish_date <= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
)
SELECT 
    video_id,
    title,
    date,
    filled_views,
    AVG(filled_views) OVER (
        PARTITION BY video_id 
        ORDER BY date
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) as 14day_moving_avg
FROM video_stats_filled
ORDER BY video_id, date;
```

**Key Learning**: LEFT JOIN with NULL handling for gap management. LAST_VALUE IGNORE NULLS for forward-fill.

---

## Solution 4: Amazon - Daily Revenue and Running Totals

**Problem**: Calculate cumulative revenue metrics for 180-day period.

```sql
SELECT 
    o.order_date,
    SUM(o.total_amount) as daily_revenue,
    SUM(SUM(o.total_amount)) OVER (
        ORDER BY o.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue,
    COUNT(DISTINCT o.order_id) OVER (
        ORDER BY o.order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_order_count,
    ROUND(
        SUM(SUM(o.total_amount)) OVER (
            ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / SUM(SUM(o.total_amount)) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) * 100, 2
    ) as percent_of_total_revenue_accrued
FROM orders o
WHERE o.order_status = 'completed'
    AND o.order_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
GROUP BY o.order_date
ORDER BY o.order_date;
```

**Explanation**:
- First SUM: Daily revenue (sum of all orders that day)
- Second SUM OVER: Cumulative revenue from start to current date
- COUNT OVER: Running count of orders (cumulative)
- Percent of total: Cumulative / total revenue * 100
- Total revenue calculated using UNBOUNDED FOLLOWING window
- Real use case: Sales reporting, revenue tracking

**More detailed breakdown**:
```sql
WITH daily_metrics AS (
    SELECT 
        o.order_date,
        COUNT(DISTINCT o.order_id) as daily_orders,
        SUM(o.total_amount) as daily_revenue,
        COUNT(DISTINCT o.customer_id) as daily_customers
    FROM orders o
    WHERE o.order_status = 'completed'
        AND o.order_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
    GROUP BY o.order_date
),
cumulative_calcs AS (
    SELECT 
        order_date,
        daily_orders,
        daily_revenue,
        daily_customers,
        SUM(daily_revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_revenue,
        SUM(daily_orders) OVER (
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_orders,
        SUM(daily_revenue) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as total_revenue_180days
    FROM daily_metrics
)
SELECT 
    order_date,
    daily_revenue,
    cumulative_revenue,
    cumulative_orders,
    ROUND(cumulative_revenue / total_revenue_180days * 100, 2) as percent_of_total,
    ROUND(daily_revenue / 180 * 100, 2) as percent_of_daily_average
FROM cumulative_calcs
ORDER BY order_date;
```

**Key Learning**: Multiple SUM OVER clauses for different windows. UNBOUNDED FOLLOWING for totals.

---

## Solution 5: Twitter - 30-Day Moving Average with Year-over-Year

**Problem**: Calculate trending with temporal comparison.

```sql
WITH daily_tweets AS (
    SELECT 
        DATE(created_at) as date,
        COUNT(*) as daily_tweet_count,
        EXTRACT(MONTH FROM created_at) as month,
        EXTRACT(YEAR FROM created_at) as year
    FROM tweets
    WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
    GROUP BY DATE(created_at)
),
with_moving_avg AS (
    SELECT 
        date,
        daily_tweet_count,
        month,
        year,
        ROUND(
            AVG(daily_tweet_count) OVER (
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 2
        ) as 30day_moving_avg
    FROM daily_tweets
),
month_aggregates AS (
    SELECT 
        CONCAT(year, '-', LPAD(month, 2, '0')) as year_month,
        AVG(daily_tweet_count) as month_avg_tweets
    FROM daily_tweets
    GROUP BY year, month
),
current_vs_prior AS (
    SELECT 
        current.year_month as current_month,
        current.month_avg_tweets as current_month_avg,
        prior.month_avg_tweets as prior_year_month_avg,
        ROUND(
            (current.month_avg_tweets - prior.month_avg_tweets) / prior.month_avg_tweets * 100, 2
        ) as yoy_change_percentage,
        CASE 
            WHEN ABS((current.month_avg_tweets - prior.month_avg_tweets) / prior.month_avg_tweets * 100) > 10 
                THEN TRUE
            ELSE FALSE
        END as is_significant_change
    FROM month_aggregates current
    LEFT JOIN month_aggregates prior 
        ON current.year_month = DATE_FORMAT(DATE_SUB(STR_TO_DATE(CONCAT(prior.year_month, '-01'), '%Y-%m-%d'), INTERVAL 1 YEAR), '%Y-%m')
)
SELECT 
    wma.date,
    wma.daily_tweet_count,
    wma.30day_moving_avg,
    wma.year_month,
    cvp.current_month_avg,
    cvp.prior_year_month_avg,
    cvp.yoy_change_percentage,
    cvp.is_significant_change
FROM with_moving_avg wma
LEFT JOIN current_vs_prior cvp ON wma.year_month = cvp.current_month
ORDER BY wma.date DESC;
```

**Explanation**:
- Daily aggregation with moving average
- Month aggregation for year-over-year comparison
- YoY change = (current - prior) / prior * 100
- Significance threshold: >10% change
- Real use case: Platform trend analysis, growth metrics

**Simpler version**:
```sql
WITH daily_tweets AS (
    SELECT 
        DATE(created_at) as date,
        COUNT(*) as daily_count
    FROM tweets
    WHERE created_at >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
    GROUP BY DATE(created_at)
)
SELECT 
    date,
    daily_count,
    ROUND(AVG(daily_count) OVER (
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) as 30day_moving_avg,
    EXTRACT(MONTH FROM date) as month,
    EXTRACT(YEAR FROM date) as year
FROM daily_tweets
ORDER BY date DESC;
```

**Key Learning**: EXTRACT for temporal components. Multiple CTEs for layered calculations. Threshold-based significance testing.

---

## Solution 6: Uber - Cumulative Earnings with Running Statistics

**Problem**: Track driver cumulative earnings with performance indicators.

```sql
WITH driver_daily AS (
    SELECT 
        d.driver_id,
        d.driver_name,
        des.date,
        des.earnings,
        des.rides_completed,
        des.hours_online,
        ROUND(des.earnings / NULLIF(des.hours_online, 0), 2) as earnings_per_hour
    FROM drivers d
    INNER JOIN daily_driver_stats des ON d.driver_id = des.driver_id
    WHERE des.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
),
with_cumulative AS (
    SELECT 
        driver_id,
        driver_name,
        date,
        earnings,
        rides_completed,
        earnings_per_hour,
        SUM(earnings) OVER (
            PARTITION BY driver_id 
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_earnings,
        ROUND(
            AVG(earnings) OVER (
                PARTITION BY driver_id 
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) as 7day_moving_avg_earnings,
        ROUND(
            SUM(earnings) OVER (
                PARTITION BY driver_id 
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / NULLIF(
                SUM(hours_online) OVER (
                    PARTITION BY driver_id 
                    ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 0
            ), 2
        ) as running_avg_earnings_per_hour,
        AVG(earnings) OVER (
            PARTITION BY driver_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as 30day_avg_earnings
    FROM driver_daily
)
SELECT 
    driver_id,
    driver_name,
    date,
    earnings as daily_earnings,
    cumulative_earnings,
    7day_moving_avg_earnings,
    running_avg_earnings_per_hour,
    CASE 
        WHEN earnings > 30day_avg_earnings THEN 'Above Average'
        WHEN earnings > 30day_avg_earnings * 0.9 THEN 'Average'
        ELSE 'Below Average'
    END as vs_30day_average
FROM with_cumulative
ORDER BY driver_id, date;
```

**Explanation**:
- SUM with UNBOUNDED PRECEDING: Cumulative from start
- AVG with 7-day window: 7-day moving average
- Nested SUM/SUM for running average per hour
- 30-day average for comparison baseline
- CASE statement for performance classification
- Real use case: Driver earnings dashboard, incentive programs

**With anomaly detection**:
```sql
WITH driver_metrics AS (
    SELECT 
        d.driver_id,
        d.driver_name,
        des.date,
        des.earnings,
        AVG(des.earnings) OVER (
            PARTITION BY d.driver_id 
            ORDER BY des.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as 30day_moving_avg,
        STDDEV(des.earnings) OVER (
            PARTITION BY d.driver_id 
            ORDER BY des.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as 30day_stddev
    FROM drivers d
    INNER JOIN daily_driver_stats des ON d.driver_id = des.driver_id
    WHERE des.date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
)
SELECT 
    driver_id,
    driver_name,
    date,
    earnings,
    30day_moving_avg,
    ROUND(ABS(earnings - 30day_moving_avg) / NULLIF(30day_stddev, 0), 2) as z_score,
    CASE 
        WHEN ABS(earnings - 30day_moving_avg) / NULLIF(30day_stddev, 0) > 2 THEN TRUE
        ELSE FALSE
    END as is_anomaly
FROM driver_metrics
ORDER BY driver_id, date;
```

**Key Learning**: Multiple window functions with different frames. STDDEV for anomaly detection.

---

## Solution 7: Google Drive - Monthly Storage Growth Tracking

**Problem**: Track storage trends with acceleration metrics.

```sql
WITH monthly_with_lag AS (
    SELECT 
        u.user_id,
        u.user_email,
        u.plan_type,
        mss.snapshot_month,
        ROUND(mss.storage_used_bytes / (1024*1024*1024), 2) as storage_gb,
        LAG(ROUND(mss.storage_used_bytes / (1024*1024*1024), 2)) OVER (
            PARTITION BY u.user_id 
            ORDER BY mss.snapshot_month
        ) as previous_month_storage_gb,
        ROUND(mss.storage_used_bytes / (1024*1024*1024)) - 
        LAG(ROUND(mss.storage_used_bytes / (1024*1024*1024))) OVER (
            PARTITION BY u.user_id 
            ORDER BY mss.snapshot_month
        ) as mom_growth_gb,
        SUM(ROUND(mss.storage_used_bytes / (1024*1024*1024), 2)) OVER (
            PARTITION BY u.user_id 
            ORDER BY mss.snapshot_month
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as cumulative_6month_growth_percent
    FROM users u
    INNER JOIN monthly_storage_snapshots mss ON u.user_id = mss.user_id
    WHERE mss.storage_used_bytes / (1024*1024*1024) > 100
),
with_percent AS (
    SELECT 
        user_id,
        user_email,
        plan_type,
        snapshot_month,
        storage_gb,
        previous_month_storage_gb,
        ROUND(
            (mom_growth_gb / NULLIF(previous_month_storage_gb, 0)) * 100, 2
        ) as mom_growth_percent,
        cumulative_6month_growth_percent,
        ROUND(storage_gb / 100 * 100, 2) as percent_of_limit_used
    FROM monthly_with_lag
)
SELECT 
    user_id,
    user_email,
    plan_type,
    snapshot_month,
    storage_gb,
    previous_month_storage_gb,
    mom_growth_percent,
    cumulative_6month_growth_percent,
    percent_of_limit_used,
    CASE 
        WHEN percent_of_limit_used >= 80 THEN TRUE
        ELSE FALSE
    END as approaching_limit
FROM with_percent
WHERE storage_gb > 1
ORDER BY user_id, snapshot_month DESC;
```

**Explanation**:
- LAG retrieves previous month storage
- MoM growth calculation: current - previous
- Cumulative 6-month: SUM of last 6 months
- Percent of limit: storage / 100 * 100 (assumes 100GB plan)
- Approaching limit: >80% threshold
- Real use case: Churn prediction, quota management

**Key Learning**: LAG for comparison, SUM OVER for rolling cumulative, NULLIF for division by zero.

---

## Solution 8: LinkedIn - Connection Growth Trajectory

**Problem**: Track acceleration in connection growth.

```sql
WITH daily_connections AS (
    SELECT 
        u.user_id,
        dcs.date,
        dcs.new_connections,
        dcs.total_connections
    FROM users u
    INNER JOIN daily_connection_stats dcs ON u.user_id = dcs.user_id
    WHERE dcs.date >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
),
with_cumulative AS (
    SELECT 
        user_id,
        date,
        new_connections,
        total_connections,
        SUM(new_connections) OVER (
            PARTITION BY user_id 
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_new_connections,
        ROUND(
            AVG(new_connections) OVER (
                PARTITION BY user_id 
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) as 7day_moving_avg_new_connections
    FROM daily_connections
),
with_acceleration AS (
    SELECT 
        user_id,
        date,
        new_connections,
        cumulative_new_connections,
        7day_moving_avg_new_connections,
        LAG(7day_moving_avg_new_connections) OVER (
            PARTITION BY user_id 
            ORDER BY date
        ) as prev_week_avg,
        CASE 
            WHEN 7day_moving_avg_new_connections > LAG(7day_moving_avg_new_connections) OVER (
                PARTITION BY user_id 
                ORDER BY date
            ) THEN 'Accelerating'
            WHEN 7day_moving_avg_new_connections < LAG(7day_moving_avg_new_connections) OVER (
                PARTITION BY user_id 
                ORDER BY date
            ) THEN 'Decelerating'
            ELSE 'Stable'
        END as weekly_acceleration_trend
    FROM with_cumulative
)
SELECT 
    user_id,
    date,
    new_connections,
    cumulative_new_connections,
    7day_moving_avg_new_connections,
    weekly_acceleration_trend
FROM with_acceleration
WHERE new_connections > 0
ORDER BY user_id, date DESC;
```

**Explanation**:
- SUM OVER: Cumulative new connections
- AVG OVER 7 days: Moving average for trend smoothing
- LAG of moving average: Previous week's trend
- Comparison creates acceleration indicator
- Real use case: Growth analysis, influencer tracking

**Key Learning**: LAG applied to aggregate results. Multiple layers of aggregation for trend analysis.

---

## Solution 9: Spotify - Artist Streaming Momentum

**Problem**: Track post-release streaming with momentum scoring.

```sql
WITH release_window AS (
    SELECT 
        a.artist_id,
        a.artist_name,
        a.release_date,
        das.date,
        das.stream_count,
        DATEDIFF(das.date, a.release_date) + 1 as days_since_release,
        SUM(das.stream_count) OVER (
            PARTITION BY a.artist_id 
            ORDER BY das.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_streams,
        ROUND(
            AVG(das.stream_count) OVER (
                PARTITION BY a.artist_id 
                ORDER BY das.date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ), 2
        ) as 14day_moving_avg,
        CASE 
            WHEN das.stream_count > 2 * AVG(das.stream_count) OVER (
                PARTITION BY a.artist_id 
                ORDER BY das.date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) THEN TRUE
            ELSE FALSE
        END as is_breakout_day
    FROM artists a
    INNER JOIN daily_artist_streams das ON a.artist_id = das.artist_id
    WHERE DATEDIFF(das.date, a.release_date) + 1 <= 30
        AND a.release_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
)
SELECT 
    artist_id,
    artist_name,
    release_date,
    days_since_release,
    stream_count as daily_streams,
    cumulative_streams,
    14day_moving_avg,
    is_breakout_day,
    ROUND(
        (cumulative_streams / NULLIF(
            SUM(stream_count) OVER (
                PARTITION BY artist_id
            ), 0
        ) * 100), 2
    ) as momentum_score
FROM release_window
ORDER BY artist_id, days_since_release;
```

**Explanation**:
- Days since release: DATEDIFF + 1 (day 1 = release day)
- Cumulative streams: SUM OVER unbounded
- 14-day moving average: Trend smoothing
- Breakout days: >2x moving average
- Momentum: Cumulative / total * 100 (% of total accrued so far)
- Real use case: Album performance, release strategy

**Key Learning**: Lifecycle analysis from fixed point (release). Momentum scoring identifies viral moments.

---

## Solution 10: Netflix - Content Lifecycle Analysis

**Problem**: Track complete 90-day lifecycle with phase detection.

```sql
WITH daily_content_stats AS (
    SELECT 
        c.content_id,
        c.content_type,
        c.release_date,
        cds.date,
        cds.views,
        DATEDIFF(cds.date, c.release_date) + 1 as days_since_release,
        SUM(cds.views) OVER (
            PARTITION BY c.content_id 
            ORDER BY cds.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_views,
        ROUND(
            AVG(cds.views) OVER (
                PARTITION BY c.content_id 
                ORDER BY cds.date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) as 7day_moving_avg
    FROM content c
    INNER JOIN content_daily_stats cds ON c.content_id = cds.content_id
    WHERE DATEDIFF(cds.date, c.release_date) + 1 <= 90
        AND c.release_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
)
SELECT 
    content_id,
    content_type,
    release_date,
    days_since_release,
    views as daily_views,
    7day_moving_avg,
    cumulative_views,
    CASE 
        WHEN days_since_release <= 7 THEN 'Launch Week'
        WHEN days_since_release <= 14 THEN 'Early Growth'
        WHEN 7day_moving_avg > (
            SELECT AVG(7day_moving_avg) 
            FROM (
                SELECT 7day_moving_avg
                FROM daily_content_stats dcs2
                WHERE dcs2.content_id = daily_content_stats.content_id
                    AND dcs2.days_since_release BETWEEN 15 AND 30
            )
        ) THEN 'Mature'
        WHEN 7day_moving_avg < LAG(7day_moving_avg) OVER (
            PARTITION BY content_id 
            ORDER BY days_since_release
        ) * 0.8 THEN 'Decline'
        ELSE 'Stable'
    END as lifecycle_phase,
    CASE 
        WHEN days_since_release <= 7 THEN 'Explosive Growth'
        WHEN days_since_release BETWEEN 8 AND 14 THEN 'Peak'
        WHEN days_since_release BETWEEN 15 AND 30 THEN 'Sustained'
        ELSE 'Long Tail'
    END as phase_indicator
FROM daily_content_stats
ORDER BY content_id, days_since_release;
```

**Explanation**:
- Days since release: Fixed reference point
- Cumulative views: Total accrued to date
- 7-day moving average: Trend line
- Lifecycle phases: Based on age and trend
- Growth rate comparison: Current vs peak
- Real use case: Content performance tiers, release optimization

**Key Learning**: Phase detection combines multiple metrics. Lifecycle analysis from fixed reference points.

---

## Performance Optimization Tips

1. **Window function indexes**: PARTITION BY and ORDER BY columns should be indexed
2. **UNBOUNDED vs ROWS**: ROWS BETWEEN is more efficient than RANGE for large datasets
3. **Materialization**: Pre-calculate moving averages in batch jobs for reporting
4. **Data volume**: Test with actual data volumes; moving avg on billions of rows can be slow
5. **CTE organization**: Break complex queries into simpler CTEs for readability and optimization

## Common Patterns Summary

| Pattern | SQL | Use Case |
|---------|-----|----------|
| 7-day moving avg | `AVG(x) OVER (...ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` | Trend smoothing |
| Cumulative sum | `SUM(x) OVER (...ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` | Running totals |
| YoY comparison | `CURRENT_YEAR / PRIOR_YEAR` | Growth rates |
| Percent of total | `CUMULATIVE / TOTAL * 100` | Progress tracking |
| Acceleration | `CURRENT_TREND > LAG(TREND)` | Growth direction |
