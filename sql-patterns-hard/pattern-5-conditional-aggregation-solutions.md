# Pattern 5: Conditional Aggregations (CASE WHEN) - Solutions

## Solution 1: Amazon - Revenue Split by Order Status

**Problem**: Calculate daily revenue breakdown by order status.

```sql
SELECT 
    o.order_date,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(CASE WHEN o.order_status = 'completed' THEN o.total_amount ELSE 0 END) as completed_revenue,
    SUM(CASE WHEN o.order_status = 'cancelled' THEN o.total_amount ELSE 0 END) as cancelled_revenue,
    SUM(CASE WHEN o.order_status = 'pending' THEN o.total_amount ELSE 0 END) as pending_revenue,
    ROUND(
        SUM(CASE WHEN o.order_status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2
    ) as cancellation_rate
FROM orders o
WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY o.order_date
HAVING COUNT(DISTINCT o.order_id) >= 100
ORDER BY o.order_date DESC;
```

**Explanation**:
- CASE WHEN conditionally includes amount in aggregation
- Each status gets separate SUM for segmented metrics
- COUNT within CASE counts rows matching condition
- Cancellation rate: cancelled count / total count * 100
- HAVING filters to high-volume days (100+ orders)
- Real use case: Financial reporting, quality control, status tracking

**More detailed with category breakdown**:
```sql
SELECT 
    o.order_date,
    oi.category,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT CASE WHEN o.order_status = 'completed' THEN o.order_id END) as completed_orders,
    COUNT(DISTINCT CASE WHEN o.order_status = 'cancelled' THEN o.order_id END) as cancelled_orders,
    SUM(CASE WHEN o.order_status = 'completed' THEN o.total_amount ELSE 0 END) as completed_revenue,
    ROUND(
        SUM(CASE WHEN o.order_status = 'cancelled' THEN o.total_amount ELSE 0 END) / 
        SUM(o.total_amount) * 100, 2
    ) as cancelled_percent_of_total,
    ROUND(
        COUNT(CASE WHEN o.order_status = 'cancelled' THEN 1 END) / COUNT(*) * 100, 2
    ) as cancellation_rate
FROM orders o
INNER JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    AND o.order_status IN ('completed', 'cancelled', 'pending')
GROUP BY o.order_date, oi.category
ORDER BY o.order_date DESC, completed_revenue DESC;
```

**Key Learning**: CASE WHEN within SUM creates segmented metrics. Multiple CASE conditions in single query for efficiency.

---

## Solution 2: Netflix - Subscriber Segmentation Metrics

**Problem**: Calculate subscription tier performance and conversion metrics.

```sql
SELECT 
    DATE_TRUNC(CURDATE(), MONTH) as period_month,
    COUNT(DISTINCT CASE WHEN u.subscription_tier = 'basic' AND u.churn_date IS NULL THEN u.user_id END) as basic_active_users,
    COUNT(DISTINCT CASE WHEN u.subscription_tier = 'standard' AND u.churn_date IS NULL THEN u.user_id END) as standard_active_users,
    COUNT(DISTINCT CASE WHEN u.subscription_tier = 'premium' AND u.churn_date IS NULL THEN u.user_id END) as premium_active_users,
    SUM(CASE WHEN b.subscription_tier = 'basic' AND b.status = 'paid' THEN b.amount_paid ELSE 0 END) as basic_revenue,
    SUM(CASE WHEN b.subscription_tier = 'standard' AND b.status = 'paid' THEN b.amount_paid ELSE 0 END) as standard_revenue,
    SUM(CASE WHEN b.subscription_tier = 'premium' AND b.status = 'paid' THEN b.amount_paid ELSE 0 END) as premium_revenue,
    ROUND(
        COUNT(DISTINCT CASE WHEN u.signup_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) 
                              AND u.subscription_tier != 'trial' THEN u.user_id END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN DATE_TRUNC(u.signup_date, MONTH) = DATE_TRUNC(CURDATE(), MONTH) 
                                      AND u.subscription_tier = 'trial' THEN u.user_id END), 0) * 100, 2
    ) as trial_conversion_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN u.churn_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN u.user_id END) / 
        NULLIF(COUNT(DISTINCT u.user_id), 0) * 100, 2
    ) as churn_rate_all_tiers
FROM users u
LEFT JOIN billing b ON u.user_id = b.user_id AND b.billing_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
WHERE u.signup_date <= CURDATE()
GROUP BY DATE_TRUNC(CURDATE(), MONTH)
ORDER BY period_month DESC;
```

**Explanation**:
- COUNT(DISTINCT CASE WHEN ... END): Counts unique users matching condition
- Multiple COUNT CASE for tier breakdown
- SUM with CASE for tier-specific revenue
- Trial conversion: new paid users / trial signups * 100
- Churn rate: churned users / total users * 100
- NULLIF prevents division by zero
- Real use case: Business metrics dashboard, subscription health

**With cohort analysis**:
```sql
WITH user_cohorts AS (
    SELECT 
        DATE_TRUNC(u.signup_date, MONTH) as signup_cohort,
        u.subscription_tier,
        COUNT(DISTINCT u.user_id) as cohort_size,
        COUNT(DISTINCT CASE WHEN u.churn_date IS NOT NULL THEN u.user_id END) as churned_count,
        SUM(CASE WHEN b.status = 'paid' THEN b.amount_paid ELSE 0 END) as total_lifetime_revenue
    FROM users u
    LEFT JOIN billing b ON u.user_id = b.user_id
    GROUP BY DATE_TRUNC(u.signup_date, MONTH), u.subscription_tier
)
SELECT 
    signup_cohort,
    subscription_tier,
    cohort_size,
    churned_count,
    ROUND(churned_count / cohort_size * 100, 2) as churn_rate_percent,
    ROUND(total_lifetime_revenue / cohort_size, 2) as avg_revenue_per_user
FROM user_cohorts
ORDER BY signup_cohort DESC, subscription_tier;
```

**Key Learning**: COUNT DISTINCT CASE for cohort analysis. Multiple conditions for segment-specific metrics.

---

## Solution 3: Uber - Ride Outcome Distribution

**Problem**: Calculate driver performance metrics by outcome type.

```sql
SELECT 
    d.driver_id,
    COUNT(DISTINCT rr.request_id) as total_requests,
    COUNT(DISTINCT CASE WHEN rr.outcome = 'accepted' THEN rr.request_id END) as accepted_count,
    COUNT(DISTINCT CASE WHEN rr.outcome = 'cancelled' THEN rr.request_id END) as cancelled_count,
    COUNT(DISTINCT CASE WHEN r.ride_id IS NOT NULL THEN rr.request_id END) as completed_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN rr.outcome = 'accepted' THEN rr.request_id END) / 
        COUNT(*) * 100, 2
    ) as acceptance_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN rr.outcome = 'cancelled' THEN rr.request_id END) / 
        COUNT(*) * 100, 2
    ) as cancellation_rate,
    ROUND(AVG(CASE WHEN r.ride_id IS NOT NULL THEN r.ride_fare ELSE NULL END), 2) as avg_completed_fare
FROM drivers d
INNER JOIN ride_requests rr ON d.driver_id = rr.driver_id
LEFT JOIN rides r ON rr.request_id = r.request_id
WHERE rr.request_time >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY d.driver_id
ORDER BY d.driver_id;
```

**Explanation**:
- COUNT DISTINCT CASE for conditional counting
- Multiple outcomes tracked separately
- Rates calculated as count / total * 100
- AVG with CASE for completed rides only (ignores NULL)
- Real use case: Driver quality metrics, performance incentives

**With quality tiers**:
```sql
WITH driver_stats AS (
    SELECT 
        d.driver_id,
        COUNT(DISTINCT rr.request_id) as total_requests,
        ROUND(COUNT(DISTINCT CASE WHEN rr.outcome = 'accepted' THEN rr.request_id END) / COUNT(*) * 100, 2) as acceptance_rate,
        ROUND(COUNT(DISTINCT CASE WHEN rr.outcome = 'cancelled' THEN rr.request_id END) / COUNT(*) * 100, 2) as cancellation_rate,
        ROUND(AVG(CASE WHEN r.ride_id IS NOT NULL THEN r.ride_fare ELSE NULL END), 2) as avg_completed_fare,
        ROUND(AVG(CASE WHEN r.ride_id IS NOT NULL THEN r.rating ELSE NULL END), 2) as avg_rating
    FROM drivers d
    INNER JOIN ride_requests rr ON d.driver_id = rr.driver_id
    LEFT JOIN rides r ON rr.request_id = r.request_id
    WHERE rr.request_time >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
        AND rr.outcome IN ('accepted', 'cancelled')
    GROUP BY d.driver_id
)
SELECT 
    driver_id,
    total_requests,
    acceptance_rate,
    cancellation_rate,
    avg_completed_fare,
    avg_rating,
    CASE 
        WHEN avg_rating >= 4.8 AND acceptance_rate >= 90 THEN 'Platinum'
        WHEN avg_rating >= 4.5 AND acceptance_rate >= 80 THEN 'Gold'
        WHEN avg_rating >= 4.0 AND acceptance_rate >= 70 THEN 'Silver'
        ELSE 'Standard'
    END as driver_tier
FROM driver_stats
ORDER BY driver_tier DESC, avg_rating DESC;
```

**Key Learning**: Multiple CASE conditions for multi-dimensional quality assessment. Tiering logic based on combined metrics.

---

## Solution 4: Instagram - Content Performance by Type

**Problem**: Compare engagement metrics across post types for high-follower accounts.

```sql
SELECT 
    u.user_id,
    p.post_type,
    COUNT(DISTINCT p.post_id) as count_posts,
    ROUND(AVG(CASE WHEN p.post_type IN ('photo', 'video', 'carousel', 'reel') THEN p.likes ELSE NULL END), 2) as avg_likes,
    ROUND(AVG(CASE WHEN p.post_type IN ('photo', 'video', 'carousel', 'reel') THEN p.comments ELSE NULL END), 2) as avg_comments,
    ROUND(AVG(CASE WHEN p.post_type IN ('photo', 'video', 'carousel', 'reel') THEN p.shares ELSE NULL END), 2) as avg_shares,
    ROUND(
        (SUM(CASE WHEN p.post_type IN ('photo', 'video', 'carousel', 'reel') THEN p.likes + p.comments + p.shares ELSE 0 END) / 
        NULLIF(COUNT(DISTINCT p.post_id), 0)), 2
    ) as engagement_rate
FROM users u
INNER JOIN posts p ON u.user_id = p.user_id
WHERE u.follower_count >= 10000
    AND p.created_at >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
GROUP BY u.user_id, p.post_type
HAVING COUNT(DISTINCT p.post_id) >= 5
ORDER BY u.user_id, engagement_rate DESC;
```

**Explanation**:
- CASE WHEN validates post_type before aggregation
- Multiple AVG CASE for different metrics
- Engagement rate: total engagement / post count
- HAVING filters to meaningful sample size (5+ posts)
- Real use case: Content strategy optimization, creator insights

**With post type comparison**:
```sql
WITH post_metrics AS (
    SELECT 
        u.user_id,
        p.post_type,
        COUNT(DISTINCT p.post_id) as count_posts,
        SUM(p.likes) as total_likes,
        SUM(p.comments) as total_comments,
        SUM(p.shares) as total_shares,
        AVG(p.likes + p.comments + p.shares) as avg_engagement_per_post
    FROM users u
    INNER JOIN posts p ON u.user_id = p.user_id
    WHERE u.follower_count >= 10000
        AND p.created_at >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY u.user_id, p.post_type
    HAVING COUNT(DISTINCT p.post_id) >= 5
),
ranked_types AS (
    SELECT 
        user_id,
        post_type,
        count_posts,
        ROUND(total_likes / count_posts, 2) as avg_likes,
        ROUND(total_comments / count_posts, 2) as avg_comments,
        ROUND(avg_engagement_per_post, 2) as avg_engagement,
        RANK() OVER (PARTITION BY user_id ORDER BY avg_engagement_per_post DESC) as engagement_rank
    FROM post_metrics
)
SELECT 
    user_id,
    post_type,
    count_posts,
    avg_likes,
    avg_comments,
    avg_engagement,
    CASE 
        WHEN engagement_rank = 1 THEN 'Best Performing'
        WHEN engagement_rank = 2 THEN 'Second Best'
        ELSE 'Other'
    END as performance_tier
FROM ranked_types
ORDER BY user_id, engagement_rank;
```

**Key Learning**: CASE WHEN for filtering data before aggregation. Ranking with CASE for tier assignment.

---

## Solution 5: Twitter - Tweet Engagement by Content Type

**Problem**: Analyze engagement rates across content types.

```sql
SELECT 
    DATE(t.created_at) as period,
    COUNT(DISTINCT t.tweet_id) as total_tweets,
    COUNT(DISTINCT CASE WHEN t.has_link = TRUE THEN t.tweet_id END) as tweet_with_link_count,
    COUNT(DISTINCT CASE WHEN t.has_image = TRUE THEN t.tweet_id END) as tweet_with_image_count,
    COUNT(DISTINCT CASE WHEN t.has_video = TRUE THEN t.tweet_id END) as tweet_with_video_count,
    ROUND(
        (SUM(t.likes + t.retweets + t.replies) / COUNT(*)), 2
    ) as overall_engagement_rate,
    ROUND(
        (SUM(CASE WHEN t.has_link = TRUE THEN t.likes + t.retweets + t.replies ELSE 0 END) / 
        NULLIF(COUNT(CASE WHEN t.has_link = TRUE THEN t.tweet_id END), 0)), 2
    ) as link_tweet_engagement_rate,
    ROUND(
        (SUM(CASE WHEN t.has_image = TRUE THEN t.likes + t.retweets + t.replies ELSE 0 END) / 
        NULLIF(COUNT(CASE WHEN t.has_image = TRUE THEN t.tweet_id END), 0)), 2
    ) as image_tweet_engagement_rate,
    ROUND(
        (SUM(CASE WHEN t.has_video = TRUE THEN t.likes + t.retweets + t.replies ELSE 0 END) / 
        NULLIF(COUNT(CASE WHEN t.has_video = TRUE THEN t.tweet_id END), 0)), 2
    ) as video_tweet_engagement_rate
FROM tweets t
WHERE t.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY DATE(t.created_at)
ORDER BY period DESC;
```

**Explanation**:
- Multiple COUNT CASE for content type breakdown
- Overall engagement: (likes + retweets + replies) / total tweets
- Content-specific engagement: conditional sum / conditional count
- NULLIF prevents division by zero for rare content types
- Real use case: Content strategy, engagement optimization

**With trend analysis**:
```sql
WITH daily_content_engagement AS (
    SELECT 
        DATE(t.created_at) as date,
        COUNT(DISTINCT t.tweet_id) as total_tweets,
        SUM(CASE WHEN t.has_link THEN 1 ELSE 0 END) as tweets_with_link,
        SUM(CASE WHEN t.has_image THEN 1 ELSE 0 END) as tweets_with_image,
        SUM(CASE WHEN t.has_video THEN 1 ELSE 0 END) as tweets_with_video,
        ROUND(AVG(CASE WHEN t.has_link THEN t.likes + t.retweets + t.replies ELSE NULL END), 2) as avg_link_engagement,
        ROUND(AVG(CASE WHEN t.has_image THEN t.likes + t.retweets + t.replies ELSE NULL END), 2) as avg_image_engagement,
        ROUND(AVG(CASE WHEN t.has_video THEN t.likes + t.retweets + t.replies ELSE NULL END), 2) as avg_video_engagement
    FROM tweets t
    WHERE t.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY DATE(t.created_at)
)
SELECT 
    date,
    total_tweets,
    tweets_with_link,
    tweets_with_image,
    tweets_with_video,
    avg_link_engagement,
    avg_image_engagement,
    avg_video_engagement,
    CASE 
        WHEN avg_video_engagement > avg_image_engagement AND avg_video_engagement > avg_link_engagement THEN 'Video'
        WHEN avg_image_engagement > avg_link_engagement THEN 'Image'
        ELSE 'Link'
    END as best_performing_content_type
FROM daily_content_engagement
ORDER BY date DESC;
```

**Key Learning**: Content type comparison with conditional aggregation. Multiple engagement calculations in single query.

---

## Solution 6: Spotify - Premium vs Free User Metrics

**Problem**: Compare premium and free user streaming patterns with conversion metrics.

```sql
SELECT 
    DATE_TRUNC(s.stream_date, MONTH) as year_month,
    COUNT(DISTINCT CASE WHEN u.user_type = 'free' THEN u.user_id END) as free_user_count,
    COUNT(DISTINCT CASE WHEN u.user_type = 'premium' THEN u.user_id END) as premium_user_count,
    SUM(CASE WHEN u.user_type = 'free' THEN s.stream_count ELSE 0 END) as free_total_streams,
    SUM(CASE WHEN u.user_type = 'premium' THEN s.stream_count ELSE 0 END) as premium_total_streams,
    ROUND(
        SUM(CASE WHEN u.user_type = 'free' THEN s.stream_count ELSE 0 END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN u.user_type = 'free' THEN u.user_id END), 0), 2
    ) as avg_streams_per_free_user,
    ROUND(
        SUM(CASE WHEN u.user_type = 'premium' THEN s.stream_count ELSE 0 END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN u.user_type = 'premium' THEN u.user_id END), 0), 2
    ) as avg_streams_per_premium_user,
    COUNT(DISTINCT CASE 
        WHEN u.user_type = 'free' AND EXISTS (
            SELECT 1 FROM users u2 WHERE u2.user_id = u.user_id AND u2.premium_start_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        ) THEN u.user_id 
    END) as free_to_premium_conversions,
    COUNT(DISTINCT CASE 
        WHEN u.user_type = 'premium' AND u.premium_end_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN u.user_id 
    END) as churn_count
FROM users u
INNER JOIN streams s ON u.user_id = s.user_id
WHERE DATE_TRUNC(s.stream_date, MONTH) >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
GROUP BY DATE_TRUNC(s.stream_date, MONTH)
ORDER BY year_month DESC;
```

**Explanation**:
- COUNT DISTINCT CASE for user segmentation
- SUM CASE for streams by user type
- Avg calculation: total streams / user count
- Conversions: free users who became premium recently
- Churn: premium users who recently ended subscription
- Real use case: Freemium analytics, conversion optimization

**With cohort retention**:
```sql
WITH user_journey AS (
    SELECT 
        u.user_id,
        u.user_type,
        DATE_TRUNC(u.signup_date, MONTH) as signup_cohort,
        CASE 
            WHEN u.user_type = 'free' THEN 'Free'
            WHEN u.premium_start_date <= DATE_ADD(DATE_TRUNC(u.signup_date, MONTH), INTERVAL 30 DAY) THEN 'Quick Convert'
            WHEN u.premium_start_date IS NOT NULL THEN 'Eventual Convert'
            ELSE 'No Conversion'
        END as conversion_type,
        COUNT(DISTINCT s.stream_id) as total_streams
    FROM users u
    LEFT JOIN streams s ON u.user_id = s.user_id
    GROUP BY u.user_id, u.user_type, u.signup_date
)
SELECT 
    signup_cohort,
    conversion_type,
    COUNT(DISTINCT user_id) as user_count,
    ROUND(AVG(total_streams), 2) as avg_streams_per_user,
    ROUND(COUNT(DISTINCT user_id) / SUM(COUNT(DISTINCT user_id)) OVER (PARTITION BY signup_cohort) * 100, 2) as percent_of_cohort
FROM user_journey
GROUP BY signup_cohort, conversion_type
ORDER BY signup_cohort DESC, conversion_type;
```

**Key Learning**: Conditional user segmentation with aggregation. Conversion metrics within aggregate context.

---

## Solution 7: YouTube - Content Performance by Channel Type

**Problem**: Compare metrics across channel types.

```sql
SELECT 
    c.channel_type,
    COUNT(DISTINCT c.channel_id) as count_channels,
    SUM(CASE WHEN v.video_id IS NOT NULL THEN 1 ELSE 0 END) as total_videos,
    ROUND(
        SUM(CASE WHEN v.video_id IS NOT NULL THEN v.views ELSE 0 END) / 
        NULLIF(COUNT(DISTINCT c.channel_id), 0), 2
    ) as avg_views_per_video,
    ROUND(
        SUM(CASE WHEN v.video_id IS NOT NULL THEN v.watch_time_hours ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN v.video_id IS NOT NULL THEN v.views ELSE 0 END), 0), 2
    ) as avg_watch_through_rate,
    ROUND(
        SUM(CASE WHEN v.video_id IS NOT NULL AND v.dislikes > 0 THEN v.likes / NULLIF(v.dislikes, 0) ELSE NULL END) / 
        NULLIF(COUNT(DISTINCT CASE WHEN v.video_id IS NOT NULL AND v.dislikes > 0 THEN v.video_id END), 0), 2
    ) as avg_like_to_dislike_ratio
FROM channels c
LEFT JOIN videos v ON c.channel_id = v.channel_id
WHERE c.created_date <= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
    AND v.published_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
GROUP BY c.channel_type
ORDER BY count_channels DESC;
```

**Explanation**:
- COUNT with CASE for conditional counting
- SUM with CASE for type-specific aggregation
- Avg views: total views / video count
- Watch-through rate: watch hours / views (time per view)
- Like-to-dislike: likes / dislikes (quality metric)
- Real use case: Creator program analytics, channel classification

**With performance tiers**:
```sql
WITH channel_metrics AS (
    SELECT 
        c.channel_id,
        c.channel_type,
        c.partner_status,
        COUNT(DISTINCT v.video_id) as total_videos,
        ROUND(AVG(v.views), 2) as avg_views_per_video,
        ROUND(SUM(v.watch_time_hours) / NULLIF(SUM(v.views), 0), 4) as avg_watch_through_rate,
        ROUND(AVG(CASE WHEN v.likes > 0 AND v.dislikes > 0 THEN v.likes / v.dislikes END), 2) as like_to_dislike_ratio
    FROM channels c
    LEFT JOIN videos v ON c.channel_id = v.channel_id
    WHERE v.published_date >= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
    GROUP BY c.channel_id, c.channel_type, c.partner_status
)
SELECT 
    channel_type,
    partner_status,
    COUNT(DISTINCT channel_id) as channel_count,
    ROUND(AVG(avg_views_per_video), 2) as median_avg_views,
    ROUND(AVG(avg_watch_through_rate), 4) as median_watch_through_rate,
    CASE 
        WHEN AVG(avg_views_per_video) > 100000 AND AVG(like_to_dislike_ratio) > 5 THEN 'Tier A'
        WHEN AVG(avg_views_per_video) > 50000 THEN 'Tier B'
        WHEN AVG(avg_views_per_video) > 10000 THEN 'Tier C'
        ELSE 'Tier D'
    END as performance_tier
FROM channel_metrics
GROUP BY channel_type, partner_status
ORDER BY channel_type, performance_tier;
```

**Key Learning**: Multi-dimensional CASE conditions for tiering. Conditional calculations for quality metrics.

---

## Solution 8: LinkedIn - User Engagement by Profile Completeness

**Problem**: Correlate profile completion with engagement patterns.

```sql
SELECT 
    CASE 
        WHEN u.profile_completion_percentage >= 75 THEN 'Q4 (75-100%)'
        WHEN u.profile_completion_percentage >= 50 THEN 'Q3 (50-75%)'
        WHEN u.profile_completion_percentage >= 25 THEN 'Q2 (25-50%)'
        ELSE 'Q1 (0-25%)'
    END as profile_completion_quartile,
    COUNT(DISTINCT u.user_id) as user_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN ua.action_type IN ('connection_request', 'profile_view', 'post_engagement', 'job_application', 'message_sent') 
            AND ua.activity_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) THEN u.user_id END) / 
        NULLIF(COUNT(DISTINCT u.user_id), 0) * 100, 2
    ) as engagement_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN ua.action_type = 'connection_request' THEN ua.activity_id END) / 
        NULLIF(COUNT(DISTINCT u.user_id), 0), 2
    ) as avg_connection_requests,
    ROUND(
        COUNT(DISTINCT CASE WHEN ua.action_type = 'post_engagement' THEN ua.activity_id END) / 
        NULLIF(COUNT(DISTINCT u.user_id), 0), 2
    ) as avg_post_engagements,
    ROUND(
        COUNT(DISTINCT CASE WHEN ua.action_type = 'message_sent' THEN ua.activity_id END) / 
        NULLIF(COUNT(DISTINCT u.user_id), 0), 2
    ) as avg_messages_sent,
    ROUND(
        SUM(CASE WHEN u.has_profile_photo = TRUE THEN 1 ELSE 0 END) + 
        SUM(CASE WHEN u.has_job_title = TRUE THEN 1 ELSE 0 END) +
        SUM(CASE WHEN u.has_industry = TRUE THEN 1 ELSE 0 END) +
        SUM(CASE WHEN u.has_skills = TRUE THEN 1 ELSE 0 END), 2
    ) as avg_profile_elements_completed
FROM users u
LEFT JOIN user_activity ua ON u.user_id = ua.user_id
WHERE ua.activity_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) OR ua.activity_date IS NULL
GROUP BY profile_completion_quartile
ORDER BY user_count DESC;
```

**Explanation**:
- CASE WHEN creates quartile assignment
- Engagement rate: users with any activity / total users
- Multiple COUNT CASE for different action types
- Avg per user: total actions / user count
- Profile elements: sum of boolean fields
- Real use case: Onboarding metrics, quality scoring

**With completeness impact analysis**:
```sql
WITH profile_metrics AS (
    SELECT 
        u.user_id,
        CASE 
            WHEN u.profile_completion_percentage >= 75 THEN 'Complete'
            WHEN u.profile_completion_percentage >= 50 THEN 'Moderate'
            ELSE 'Minimal'
        END as profile_status,
        CASE WHEN ua.user_id IS NOT NULL THEN 1 ELSE 0 END as has_activity,
        COUNT(DISTINCT CASE WHEN ua.action_type = 'connection_request' THEN ua.activity_id END) as connection_requests,
        COUNT(DISTINCT CASE WHEN ua.action_type = 'job_application' THEN ua.activity_id END) as job_applications
    FROM users u
    LEFT JOIN user_activity ua ON u.user_id = ua.user_id
        AND ua.activity_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY u.user_id
)
SELECT 
    profile_status,
    COUNT(DISTINCT user_id) as user_count,
    SUM(has_activity) as active_users,
    ROUND(AVG(connection_requests), 2) as avg_connections,
    ROUND(AVG(job_applications), 2) as avg_job_apps,
    ROUND(SUM(has_activity) / COUNT(*) * 100, 2) as activity_rate_percent
FROM profile_metrics
GROUP BY profile_status
ORDER BY user_count DESC;
```

**Key Learning**: CASE WHEN for quartile assignment. Conditional engagement metrics reveal correlation with profile completeness.

---

## Solution 9: Amazon - Purchase Behavior Segmentation

**Problem**: Segment customers by value and purchase frequency.

```sql
WITH customer_metrics AS (
    SELECT 
        c.customer_id,
        SUM(CASE WHEN o.status = 'completed' THEN o.total_amount ELSE 0 END) as lifetime_value,
        COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END) as completed_orders,
        COUNT(DISTINCT CASE WHEN o.status = 'cancelled' THEN o.order_id END) as cancelled_orders,
        ROUND(
            COUNT(DISTINCT CASE WHEN o.status = 'cancelled' THEN o.order_id END) / 
            NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 2
        ) as order_cancellation_rate,
        ROUND(
            SUM(CASE WHEN o.status = 'completed' THEN o.total_amount ELSE 0 END) / 
            NULLIF(COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END), 0), 2
        ) as avg_order_value,
        COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN oi.category END) as unique_product_categories
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
    GROUP BY c.customer_id
)
SELECT 
    customer_id,
    lifetime_value,
    completed_orders,
    cancelled_orders,
    order_cancellation_rate,
    avg_order_value,
    unique_product_categories,
    CASE 
        WHEN lifetime_value > 5000 THEN 'High-Value'
        WHEN lifetime_value > 1000 THEN 'Medium-Value'
        ELSE 'Low-Value'
    END as value_segment,
    CASE 
        WHEN completed_orders >= 10 THEN 'High-Frequency'
        WHEN completed_orders >= 3 THEN 'Medium-Frequency'
        ELSE 'Low-Frequency'
    END as frequency_segment
FROM customer_metrics
ORDER BY lifetime_value DESC;
```

**Explanation**:
- SUM CASE for conditional revenue (completed orders only)
- Multiple COUNT CASE for order status breakdown
- Cancellation rate: cancelled / total * 100
- Avg order value: total value / completed count
- CASE statements for value and frequency tiers
- Real use case: Customer segmentation, marketing targeting

**With RFM analysis**:
```sql
WITH customer_rfm AS (
    SELECT 
        c.customer_id,
        DATEDIFF(CURDATE(), MAX(o.order_date)) as recency_days,
        COUNT(DISTINCT CASE WHEN o.status = 'completed' THEN o.order_id END) as frequency,
        ROUND(SUM(CASE WHEN o.status = 'completed' THEN o.total_amount ELSE 0 END), 2) as monetary_value
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= DATE_SUB(CURDATE(), INTERVAL 365 DAY)
    GROUP BY c.customer_id
),
rfm_ranked AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary_value,
        NTILE(5) OVER (ORDER BY recency_days DESC) as recency_rank,
        NTILE(5) OVER (ORDER BY frequency) as frequency_rank,
        NTILE(5) OVER (ORDER BY monetary_value) as monetary_rank
    FROM customer_rfm
)
SELECT 
    customer_id,
    recency_rank,
    frequency_rank,
    monetary_rank,
    CASE 
        WHEN recency_rank >= 4 AND frequency_rank >= 4 AND monetary_rank >= 4 THEN 'Champions'
        WHEN recency_rank >= 3 AND frequency_rank >= 3 AND monetary_rank >= 3 THEN 'Loyal'
        WHEN monetary_rank >= 4 THEN 'Big Spenders'
        WHEN frequency_rank >= 4 THEN 'Frequent Buyers'
        ELSE 'At Risk'
    END as customer_segment
FROM rfm_ranked
ORDER BY customer_id;
```

**Key Learning**: Multi-dimension segmentation with CASE WHEN. RFM analysis for customer value assessment.

---

## Solution 10: WhatsApp - Message Quality & Engagement Metrics

**Problem**: Analyze message characteristics and engagement patterns by chat type.

```sql
SELECT 
    c.chat_type,
    COUNT(DISTINCT m.message_id) as total_messages,
    ROUND(AVG(LENGTH(m.message_text)), 2) as avg_message_length,
    ROUND(
        COUNT(DISTINCT CASE WHEN m.has_emoji = TRUE THEN m.message_id END) / 
        NULLIF(COUNT(*), 0) * 100, 2
    ) as percent_with_emoji,
    ROUND(
        COUNT(DISTINCT CASE WHEN m.has_media = TRUE THEN m.message_id END) / 
        NULLIF(COUNT(*), 0) * 100, 2
    ) as percent_with_media,
    ROUND(
        COUNT(DISTINCT CASE WHEN m.has_link = TRUE THEN m.message_id END) / 
        NULLIF(COUNT(*), 0) * 100, 2
    ) as percent_with_link,
    ROUND(AVG(CASE WHEN m.reaction_count > 0 THEN m.reaction_count ELSE 0 END), 2) as avg_reactions_per_message,
    ROUND(AVG(m.read_count), 2) as avg_read_count,
    CASE 
        WHEN AVG(m.reaction_count) > 2 AND AVG(m.read_count) > 5 THEN 'High'
        WHEN AVG(m.reaction_count) > 1 OR AVG(m.read_count) > 3 THEN 'Medium'
        ELSE 'Low'
    END as engagement_level
FROM messages m
INNER JOIN chats c ON m.chat_id = c.chat_id
WHERE m.sent_timestamp >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY c.chat_type
ORDER BY total_messages DESC;
```

**Explanation**:
- COUNT CASE for feature breakdown (emoji, media, links)
- Percentages: feature count / total * 100
- AVG with CASE for conditional metrics
- Engagement level based on reaction and read counts
- Real use case: Communication pattern analysis, feature usage

**With user behavior analysis**:
```sql
WITH message_characteristics AS (
    SELECT 
        m.sender_id,
        c.chat_type,
        COUNT(*) as message_count,
        ROUND(AVG(LENGTH(m.message_text)), 2) as avg_message_length,
        SUM(CASE WHEN m.has_emoji THEN 1 ELSE 0 END) as emoji_count,
        SUM(CASE WHEN m.has_media THEN 1 ELSE 0 END) as media_count,
        SUM(CASE WHEN m.has_link THEN 1 ELSE 0 END) as link_count,
        ROUND(AVG(m.reaction_count), 2) as avg_reactions
    FROM messages m
    INNER JOIN chats c ON m.chat_id = c.chat_id
    WHERE m.sent_timestamp >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY m.sender_id, c.chat_type
)
SELECT 
    sender_id,
    chat_type,
    message_count,
    avg_message_length,
    ROUND(emoji_count / message_count * 100, 2) as emoji_usage_percent,
    ROUND(media_count / message_count * 100, 2) as media_usage_percent,
    CASE 
        WHEN avg_message_length > 50 THEN 'Verbose'
        WHEN avg_message_length > 20 THEN 'Conversational'
        ELSE 'Concise'
    END as communication_style,
    CASE 
        WHEN emoji_count > message_count * 0.1 THEN 'High Emoji User'
        ELSE 'Standard'
    END as user_type
FROM message_characteristics
WHERE message_count >= 10
ORDER BY message_count DESC;
```

**Key Learning**: Feature percentages using CASE within COUNT and SUM. User behavior classification based on message characteristics.

---

## Performance & Best Practices

1. **Nested CASE statements**: Can become complex; use CASE when else END for clarity
2. **NULL handling**: CASE treats NULL as missing value; use NULLIF for division safety
3. **Multiple conditions**: Limit to 3-4 major conditions for readability
4. **Aggregation efficiency**: Calculate derived metrics (rates, percentages) in SELECT rather than pre-calculated columns
5. **Testing**: Verify each CASE condition returns expected data types

## Summary Table: CASE WHEN Patterns

| Pattern | Use Case | Example |
|---------|----------|---------|
| `SUM(CASE WHEN...)` | Segmented totals | Revenue by status |
| `COUNT(CASE WHEN...)` | Conditional counting | Count by category |
| `AVG(CASE WHEN...)` | Selective averaging | Avg for specific group |
| `MAX/MIN(CASE WHEN...)` | Conditional extremes | Highest value by type |
| `CASE in WHERE` | Complex filtering | Filter after aggregate |
