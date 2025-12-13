# Pattern 2: Filtering with Subqueries - Solutions

## Solution 1: Twitter - Users Who Never Posted

**Problem**: Find users with followers who have never posted a tweet.

```sql
SELECT 
    u.user_id,
    u.user_name,
    COUNT(f.follower_id) as follower_count
FROM users u
INNER JOIN followers f ON u.user_id = f.user_id
WHERE u.user_id NOT IN (
    SELECT DISTINCT user_id 
    FROM tweets
)
GROUP BY u.user_id, u.user_name
ORDER BY follower_count DESC;
```

**Explanation**:
- NOT IN subquery filters users who never appear in tweets table
- INNER JOIN with followers ensures we only get users with followers
- GROUP BY counts followers per user
- Subquery runs once (non-correlated) and returns list of tweet authors
- Real use case: Identifying dormant influencers or accounts needing activation

**Alternative using NOT EXISTS** (often more efficient):
```sql
SELECT 
    u.user_id,
    u.user_name,
    COUNT(f.follower_id) as follower_count
FROM users u
INNER JOIN followers f ON u.user_id = f.user_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM tweets t 
    WHERE t.user_id = u.user_id
)
GROUP BY u.user_id, u.user_name
ORDER BY follower_count DESC;
```

**Why NOT EXISTS is better**: 
- EXISTS stops searching after finding first match (more efficient)
- NOT EXISTS doesn't create a full list in memory
- Correlated subquery can use user table indexes

**Key Learning**: NOT IN vs NOT EXISTS trade-off. EXISTS is more efficient for large datasets.

---

## Solution 2: Instagram - Posts Without Comments Below Average

**Problem**: Find posts with zero comments AND below-average likes.

```sql
SELECT 
    p.post_id,
    p.user_id,
    p.likes,
    (SELECT AVG(likes) FROM posts) as avg_likes_all_posts
FROM posts p
WHERE p.post_id NOT IN (
    SELECT DISTINCT post_id 
    FROM comments
)
AND p.likes < (
    SELECT AVG(likes) 
    FROM posts
)
ORDER BY p.likes ASC;
```

**Explanation**:
- First subquery (NOT IN) filters posts with zero comments
- Second subquery (scalar) calculates average likes across all posts
- WHERE clause filters on both conditions (zero comments AND below average)
- Non-correlated subqueries: average is calculated once
- Real use case: Identifying low-performing content for removal or reposts

**Alternative optimized version**:
```sql
WITH avg_likes_cte AS (
    SELECT AVG(likes) as avg_likes
    FROM posts
),
posts_with_comments AS (
    SELECT DISTINCT post_id
    FROM comments
)
SELECT 
    p.post_id,
    p.user_id,
    p.likes,
    a.avg_likes
FROM posts p
CROSS JOIN avg_likes_cte a
WHERE p.post_id NOT IN (SELECT post_id FROM posts_with_comments)
AND p.likes < a.avg_likes
ORDER BY p.likes ASC;
```

**Why CTE is better**: Subqueries are calculated once and reused, improving readability.

**Key Learning**: Multiple subqueries can be combined. CTEs often provide better performance and readability.

---

## Solution 3: Amazon - Products Never Purchased

**Problem**: Find products in catalog never purchased.

```sql
SELECT 
    p.product_id,
    p.product_name,
    p.category_id,
    DATEDIFF(CURDATE(), p.created_date) as days_since_creation
FROM products p
WHERE p.product_id NOT IN (
    SELECT DISTINCT product_id 
    FROM order_items
)
ORDER BY p.created_date ASC;
```

**Explanation**:
- Subquery returns all products that have been ordered
- NOT IN filters to products NOT in that list
- DATEDIFF calculates days since product was created
- Simple, non-correlated subquery
- Real use case: Identifying dead stock for removal, poor catalog management

**With additional business logic**:
```sql
SELECT 
    p.product_id,
    p.product_name,
    p.category_id,
    DATEDIFF(CURDATE(), p.created_date) as days_since_creation,
    CASE 
        WHEN DATEDIFF(CURDATE(), p.created_date) > 365 THEN 'Old'
        WHEN DATEDIFF(CURDATE(), p.created_date) > 90 THEN 'Stale'
        ELSE 'New'
    END as age_category
FROM products p
WHERE p.product_id NOT IN (
    SELECT DISTINCT product_id 
    FROM order_items
    WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
)
ORDER BY p.created_date ASC;
```

**Key Learning**: Subqueries can be used for clean inclusion/exclusion logic. Business context determines thresholds.

---

## Solution 4: Netflix - Users Watching Unauthorized Content

**Problem**: Find users watching content outside their subscription tier.

```sql
SELECT 
    u.user_id,
    GROUP_CONCAT(c.title SEPARATOR ', ') as unauthorized_content_titles
FROM users u
INNER JOIN watch_history wh ON u.user_id = wh.user_id
INNER JOIN content c ON wh.content_id = c.content_id
WHERE u.subscription_tier = 'basic'
AND c.required_tier IN (
    SELECT DISTINCT required_tier 
    FROM content 
    WHERE required_tier > u.subscription_tier
)
GROUP BY u.user_id
HAVING COUNT(*) > 0
ORDER BY u.user_id;
```

**Explanation**:
- Compares user's subscription tier with content's required tier
- Subquery identifies all premium/higher tiers than user has
- GROUP_CONCAT aggregates titles for cleaner output
- HAVING count > 0 ensures users with unauthorized views
- Real use case: Compliance, audit, and access control

**Better approach using subquery with comparison**:
```sql
SELECT DISTINCT
    u.user_id,
    u.subscription_tier,
    c.content_id,
    c.title,
    c.required_tier
FROM users u
INNER JOIN watch_history wh ON u.user_id = wh.user_id
INNER JOIN content c ON wh.content_id = c.content_id
WHERE EXISTS (
    SELECT 1
    FROM (
        SELECT 'basic' as tier_level, 1 as tier_rank
        UNION ALL
        SELECT 'standard', 2
        UNION ALL
        SELECT 'premium', 3
    ) tier_mapping ut
    INNER JOIN (
        SELECT 'basic' as tier_level, 1 as tier_rank
        UNION ALL
        SELECT 'standard', 2
        UNION ALL
        SELECT 'premium', 3
    ) tier_mapping ct ON ct.tier_level = c.required_tier
    WHERE ut.tier_level = u.subscription_tier
    AND ct.tier_rank > ut.tier_rank
)
ORDER BY u.user_id;
```

**Key Learning**: Tier comparisons need explicit mapping. EXISTS is efficient for compliance checks.

---

## Solution 5: Spotify - Artists Whose Top Track Underperforms

**Problem**: Find artists where top track streams < average artist streams.

```sql
SELECT 
    a.artist_id,
    a.artist_name,
    t_top.track_name as top_track_name,
    t_top.streams as top_track_streams,
    avg_streams.avg_streams as avg_streams_per_track,
    (avg_streams.avg_streams - t_top.streams) as gap
FROM artists a
INNER JOIN (
    SELECT artist_id, MAX(streams) as max_streams
    FROM tracks
    GROUP BY artist_id
) max_track ON a.artist_id = max_track.artist_id
INNER JOIN tracks t_top ON max_track.artist_id = t_top.artist_id 
    AND max_track.max_streams = t_top.streams
INNER JOIN (
    SELECT artist_id, AVG(streams) as avg_streams
    FROM tracks
    GROUP BY artist_id
) avg_streams ON a.artist_id = avg_streams.artist_id
WHERE avg_streams.avg_streams > t_top.streams
ORDER BY gap DESC;
```

**Explanation**:
- First subquery finds max streams per artist (top track)
- Second subquery calculates average streams per artist
- Both subqueries are correlated to artist_id
- WHERE filters: only artists where avg > top track (anomaly detection)
- Real use case: Identifying inconsistent popularity (potential data quality issues or anomalies)

**Simpler CTE approach**:
```sql
WITH artist_stats AS (
    SELECT 
        artist_id,
        MAX(streams) as top_streams,
        AVG(streams) as avg_streams
    FROM tracks
    GROUP BY artist_id
)
SELECT 
    a.artist_id,
    a.artist_name,
    t.track_name,
    t.streams as top_track_streams,
    s.avg_streams,
    (s.avg_streams - t.streams) as gap
FROM artists a
INNER JOIN artist_stats s ON a.artist_id = s.artist_id
INNER JOIN tracks t ON a.artist_id = t.artist_id 
    AND t.streams = s.top_streams
WHERE s.avg_streams > t.streams
ORDER BY gap DESC;
```

**Key Learning**: Window functions or CTEs are cleaner than multiple subqueries. Business logic: detecting anomalies requires both min and max comparisons.

---

## Solution 6: YouTube - Viewers Loyal to Creators

**Problem**: Find viewers watching same creator on 5+ different days within 30 days.

```sql
SELECT 
    wh.user_id,
    ch.creator_id,
    chn.channel_name,
    COUNT(DISTINCT DATE(wh.watched_date)) as days_watched_on,
    MIN(DATE(wh.watched_date)) as date_range_start,
    MAX(DATE(wh.watched_date)) as date_range_end
FROM watch_history wh
INNER JOIN videos v ON wh.video_id = v.video_id
INNER JOIN channels chn ON v.channel_id = chn.channel_id
WHERE wh.user_id IN (
    SELECT wh2.user_id
    FROM watch_history wh2
    INNER JOIN videos v2 ON wh2.video_id = v2.video_id
    INNER JOIN channels chn2 ON v2.channel_id = chn2.channel_id
    WHERE wh2.watched_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY wh2.user_id, chn2.channel_id
    HAVING COUNT(DISTINCT DATE(wh2.watched_date)) >= 5
)
AND wh.watched_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY wh.user_id, chn.channel_id, chn.channel_name
HAVING COUNT(DISTINCT DATE(wh.watched_date)) >= 5
ORDER BY days_watched_on DESC;
```

**Explanation**:
- Subquery identifies user-creator pairs with 5+ distinct viewing days
- Main query joins back to get content details
- `COUNT(DISTINCT DATE(...))` counts unique days per user-creator pair
- 30-day window filters recent behavior
- Real use case: Loyalty analysis, viewer habit identification

**More efficient with window functions**:
```sql
WITH viewer_loyalty AS (
    SELECT 
        wh.user_id,
        chn.channel_id,
        chn.channel_name,
        COUNT(DISTINCT DATE(wh.watched_date)) as distinct_days
    FROM watch_history wh
    INNER JOIN videos v ON wh.video_id = v.video_id
    INNER JOIN channels chn ON v.channel_id = chn.channel_id
    WHERE wh.watched_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    GROUP BY wh.user_id, chn.channel_id, chn.channel_name
    HAVING COUNT(DISTINCT DATE(wh.watched_date)) >= 5
)
SELECT 
    vl.user_id,
    vl.channel_id,
    vl.channel_name,
    vl.distinct_days,
    MIN(wh.watched_date) as date_range_start,
    MAX(wh.watched_date) as date_range_end
FROM viewer_loyalty vl
INNER JOIN watch_history wh ON vl.user_id = wh.user_id
INNER JOIN videos v ON wh.video_id = v.video_id
INNER JOIN channels chn ON v.channel_id = chn.channel_id 
    AND chn.channel_id = vl.channel_id
GROUP BY vl.user_id, vl.channel_id, vl.channel_name, vl.distinct_days
ORDER BY distinct_days DESC;
```

**Key Learning**: Subqueries for filtering are cleaner than complex HAVING. CTEs improve readability for complex multi-step logic.

---

## Solution 7: Uber - High-Performing Drivers

**Problem**: Find drivers with above-average acceptance rate, fare, and 6+ months tenure.

```sql
SELECT 
    d.driver_id,
    d.driver_name,
    d.vehicle_type,
    DATEDIFF(CURDATE(), d.signup_date) / 30.0 as months_as_driver,
    acceptance_stats.acceptance_rate,
    fare_stats.avg_fare
FROM drivers d
WHERE d.driver_id IN (
    SELECT rr.driver_id
    FROM ride_requests rr
    WHERE rr.request_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY rr.driver_id
    HAVING SUM(CASE WHEN rr.accepted = TRUE THEN 1 ELSE 0 END) / COUNT(*) > (
        SELECT AVG(acceptance_rate)
        FROM (
            SELECT 
                driver_id,
                SUM(CASE WHEN accepted = TRUE THEN 1 ELSE 0 END) / COUNT(*) as acceptance_rate
            FROM ride_requests
            WHERE request_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
            GROUP BY driver_id
        ) avg_calc
    )
)
AND d.signup_date <= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
INNER JOIN (
    SELECT 
        driver_id,
        AVG(CASE WHEN rides.ride_id IS NOT NULL THEN rides.fare_amount ELSE 0 END) as avg_fare
    FROM ride_requests rr
    LEFT JOIN rides ON rr.request_id = rides.request_id
    WHERE rr.request_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY rr.driver_id
) fare_stats ON d.driver_id = fare_stats.driver_id
INNER JOIN (
    SELECT 
        driver_id,
        SUM(CASE WHEN accepted = TRUE THEN 1 ELSE 0 END) / COUNT(*) as acceptance_rate
    FROM ride_requests
    WHERE request_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY driver_id
) acceptance_stats ON d.driver_id = acceptance_stats.driver_id
WHERE fare_stats.avg_fare > (
    SELECT AVG(fare_amount)
    FROM rides
    WHERE ride_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
)
ORDER BY acceptance_stats.acceptance_rate DESC, fare_stats.avg_fare DESC;
```

**Explanation**:
- Multiple subqueries calculate different metrics
- IN clause filters drivers with above-average acceptance rate
- Additional WHERE filters for tenure (6+ months)
- JOINs add acceptance rate and average fare stats
- DATEDIFF calculates months of experience
- Real use case: Driver incentive programs, quality metrics

**Cleaner CTE version**:
```sql
WITH driver_acceptance AS (
    SELECT 
        rr.driver_id,
        SUM(CASE WHEN rr.accepted = TRUE THEN 1 ELSE 0 END) / COUNT(*) as acceptance_rate
    FROM ride_requests rr
    WHERE rr.request_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY rr.driver_id
),
driver_fares AS (
    SELECT 
        rr.driver_id,
        AVG(r.fare_amount) as avg_fare
    FROM ride_requests rr
    INNER JOIN rides r ON rr.request_id = r.request_id
    WHERE rr.request_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
    GROUP BY rr.driver_id
),
avg_metrics AS (
    SELECT 
        AVG(da.acceptance_rate) as avg_acceptance,
        AVG(df.avg_fare) as avg_fare_platform
    FROM driver_acceptance da
    CROSS JOIN driver_fares df
)
SELECT 
    d.driver_id,
    d.driver_name,
    d.vehicle_type,
    DATEDIFF(CURDATE(), d.signup_date) / 30.0 as months_as_driver,
    da.acceptance_rate,
    df.avg_fare
FROM drivers d
INNER JOIN driver_acceptance da ON d.driver_id = da.driver_id
INNER JOIN driver_fares df ON d.driver_id = df.driver_id
CROSS JOIN avg_metrics am
WHERE d.signup_date <= DATE_SUB(CURDATE(), INTERVAL 180 DAY)
AND da.acceptance_rate > am.avg_acceptance
AND df.avg_fare > am.avg_fare_platform
ORDER BY da.acceptance_rate DESC, df.avg_fare DESC;
```

**Key Learning**: Multiple filtering conditions need organized subqueries. CTEs make complex logic clearer and testable.

---

## Solution 8: Google Docs - Documents Edited More by Non-Creators

**Problem**: Find documents edited more by non-creators than creators.

```sql
SELECT 
    d.doc_id,
    d.title,
    d.creator_id,
    non_creator_editor.editor_id,
    creator_edit_count.edit_count as creator_edit_count,
    non_creator_edits.edit_count as non_creator_edit_count
FROM documents d
INNER JOIN (
    SELECT 
        doc_id,
        editor_id,
        COUNT(*) as edit_count
    FROM document_edits
    WHERE editor_id <> (
        SELECT creator_id FROM documents WHERE doc_id = document_edits.doc_id
    )
    GROUP BY doc_id, editor_id
    HAVING COUNT(*) > (
        SELECT COUNT(*)
        FROM document_edits
        WHERE doc_id = documents.doc_id 
        AND editor_id = documents.creator_id
    )
) non_creator_edits ON d.doc_id = non_creator_edits.doc_id
INNER JOIN (
    SELECT 
        doc_id,
        COUNT(*) as edit_count
    FROM document_edits
    GROUP BY doc_id
) all_edits ON d.doc_id = all_edits.doc_id
WHERE d.doc_id IN (
    SELECT de.doc_id
    FROM document_edits de
    WHERE de.editor_id <> (
        SELECT creator_id FROM documents WHERE doc_id = de.doc_id
    )
)
ORDER BY d.doc_id;
```

**Explanation**:
- Correlated subqueries compare creator edits with non-creator edits
- WHERE editor_id <> creator_id filters out creator's own edits
- HAVING compares non-creator edits against creator's count
- Complex correlated logic for collaboration analysis
- Real use case: Identifying collaborative documents, co-authorship patterns

**Cleaner approach**:
```sql
WITH document_stats AS (
    SELECT 
        de.doc_id,
        de.editor_id,
        d.creator_id,
        COUNT(*) as edit_count,
        CASE WHEN de.editor_id = d.creator_id THEN 'creator' ELSE 'non_creator' END as editor_type
    FROM document_edits de
    INNER JOIN documents d ON de.doc_id = d.doc_id
    GROUP BY de.doc_id, de.editor_id, d.creator_id
)
SELECT 
    creator_stats.doc_id,
    (SELECT title FROM documents WHERE doc_id = creator_stats.doc_id) as title,
    creator_stats.creator_id,
    non_creator_stats.editor_id,
    creator_stats.edit_count as creator_edit_count,
    non_creator_stats.edit_count as non_creator_edit_count
FROM (
    SELECT doc_id, creator_id, editor_id, edit_count
    FROM document_stats
    WHERE editor_type = 'creator'
) creator_stats
INNER JOIN (
    SELECT doc_id, creator_id, editor_id, edit_count
    FROM document_stats
    WHERE editor_type = 'non_creator'
) non_creator_stats ON creator_stats.doc_id = non_creator_stats.doc_id
WHERE non_creator_stats.edit_count > creator_stats.edit_count
ORDER BY creator_stats.doc_id;
```

**Key Learning**: Correlated subqueries compare rows; CTEs with CASE statements are often clearer.

---

## Solution 9: LinkedIn - Eligible Users Not Applying for Jobs

**Problem**: Find job postings with eligible non-applicants.

```sql
SELECT 
    jp.job_id,
    jp.job_title,
    jp.required_experience_years,
    COUNT(DISTINCT eligible_users.user_id) as eligible_user_count,
    COUNT(DISTINCT CASE 
        WHEN eligible_users.user_id NOT IN (
            SELECT user_id FROM applications WHERE job_id = jp.job_id
        ) THEN eligible_users.user_id
    END) as users_who_did_not_apply
FROM job_postings jp
INNER JOIN (
    SELECT 
        jp2.job_id,
        u.user_id
    FROM job_postings jp2
    CROSS JOIN users u
    WHERE u.experience_years >= jp2.required_experience_years
) eligible_users ON jp.job_id = eligible_users.job_id
GROUP BY jp.job_id, jp.job_title, jp.required_experience_years
HAVING COUNT(DISTINCT eligible_users.user_id) >= 10
    AND COUNT(DISTINCT CASE 
        WHEN eligible_users.user_id NOT IN (
            SELECT user_id FROM applications WHERE job_id = jp.job_id
        ) THEN eligible_users.user_id
    END) > 0
ORDER BY users_who_did_not_apply DESC;
```

**Explanation**:
- CROSS JOIN creates all eligible user-job combinations
- IN clause filters to applied users, NOT IN gets non-applicants
- CASE WHEN counts only non-applicants
- HAVING ensures minimum eligible pool and at least 1 non-applicant
- Real use case: Job matching improvement, recruitment analytics

**With explicit NOT IN subquery**:
```sql
WITH eligible_users AS (
    SELECT 
        jp.job_id,
        u.user_id,
        u.user_name
    FROM job_postings jp
    CROSS JOIN users u
    WHERE u.experience_years >= jp.required_experience_years
),
applicants AS (
    SELECT DISTINCT
        job_id,
        user_id
    FROM applications
)
SELECT 
    jp.job_id,
    jp.job_title,
    jp.required_experience_years,
    COUNT(DISTINCT eu.user_id) as eligible_user_count,
    COUNT(DISTINCT CASE WHEN a.user_id IS NULL THEN eu.user_id END) as users_who_did_not_apply
FROM job_postings jp
INNER JOIN eligible_users eu ON jp.job_id = eu.job_id
LEFT JOIN applicants a ON eu.job_id = a.job_id AND eu.user_id = a.user_id
GROUP BY jp.job_id, jp.job_title, jp.required_experience_years
HAVING COUNT(DISTINCT eu.user_id) >= 10
ORDER BY users_who_did_not_apply DESC;
```

**Key Learning**: CROSS JOIN + filtering creates logical combinations. LEFT JOIN with NULL checks is often more efficient than NOT IN.

---

## Solution 10: WhatsApp - Silent Members in Active Groups

**Problem**: Find members never messaging in groups with 1000+ messages.

```sql
SELECT 
    gm.user_id,
    COUNT(DISTINCT g.group_id) as group_count,
    g.group_name,
    COUNT(m.message_id) as total_group_messages,
    DATEDIFF(CURDATE(), gm.added_date) as membership_duration_days,
    0 as messages_sent_by_user
FROM group_members gm
INNER JOIN groups g ON gm.group_id = g.group_id
INNER JOIN messages m ON g.group_id = m.group_id
WHERE gm.user_id NOT IN (
    SELECT DISTINCT sender_id 
    FROM messages 
    WHERE group_id = gm.group_id
)
AND g.group_id IN (
    SELECT group_id 
    FROM messages 
    GROUP BY group_id 
    HAVING COUNT(*) > 1000
)
GROUP BY gm.user_id, g.group_id, g.group_name, gm.added_date
ORDER BY membership_duration_days DESC;
```

**Explanation**:
- First NOT IN filters to users who never sent messages in that group
- Second IN filters to groups with 1000+ total messages
- Correlated logic: NOT IN references current group in WHERE
- DATEDIFF calculates engagement window
- Real use case: Group engagement analysis, spam detection, moderation

**More efficient with NOT EXISTS**:
```sql
SELECT 
    gm.user_id,
    g.group_id,
    g.group_name,
    (SELECT COUNT(*) FROM messages WHERE group_id = g.group_id) as total_group_messages,
    DATEDIFF(CURDATE(), gm.added_date) as membership_duration_days,
    0 as messages_sent_by_user
FROM group_members gm
INNER JOIN groups g ON gm.group_id = g.group_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM messages m
    WHERE m.group_id = gm.group_id 
    AND m.sender_id = gm.user_id
)
AND (
    SELECT COUNT(*) 
    FROM messages 
    WHERE group_id = gm.group_id
) > 1000
GROUP BY gm.user_id, g.group_id, g.group_name, gm.added_date
HAVING DATEDIFF(CURDATE(), MIN(gm.added_date)) > 0
ORDER BY membership_duration_days DESC;
```

**Key Learning**: NOT EXISTS with correlated subqueries is typically more efficient than NOT IN for complex multi-group scenarios.

---

## Performance Tips for Subquery Patterns

1. **Use EXISTS instead of IN**: EXISTS stops on first match, IN needs full list
2. **Scalar subqueries**: Can reference outer query (correlated), but run multiple times
3. **IN with large lists**: Consider JOIN instead for better query optimizer
4. **NOT IN with NULL**: Returns no results if subquery contains NULL (use NOT EXISTS)
5. **Subquery filtering**: Apply WHERE in subquery to reduce result set
6. **CTE reusability**: Multiple references to CTE calculated only once

## Common Pitfalls

1. **NOT IN with NULL values**: Will return no rows (use NOT EXISTS instead)
2. **Correlated subquery performance**: Runs per row; consider JOIN if possible
3. **Subquery result size**: Large results can overflow memory (use IN carefully)
4. **Multiple conditions**: Complex AND/OR logic is clearer with CTEs
