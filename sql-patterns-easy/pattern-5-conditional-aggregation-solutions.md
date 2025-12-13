# Pattern 5: Conditional Aggregation with CASE WHEN - Easy Solutions

## Solution Walkthrough

Conditional aggregation combines CASE WHEN with aggregate functions to compute multiple metrics efficiently. This is the most practical pattern in real dashboards.

---

## Problem 1 Solution: Twitter - Tweet Engagement by Type

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN is_retweet = 0 THEN 1 END) as total_original_tweets,
    COUNT(CASE WHEN is_retweet = 1 THEN 1 END) as total_retweets
FROM tweets
WHERE created_date = CURDATE();
```

**Explanation:**
- `COUNT(CASE WHEN condition THEN 1 END)` counts rows matching condition
- When condition is false, CASE returns NULL (not counted)
- Example logic:
  ```
  is_retweet = 0: CASE returns 1 → COUNT counts it
  is_retweet = 1: CASE returns NULL → COUNT ignores it
  ```
- Output: Single row with both metrics

**Key Learning:**
- This replaces TWO separate queries with ONE
- More efficient than writing two COUNT() queries separately
- All in one row makes comparisons easy

---

## Problem 2 Solution: Instagram - Post Engagement by Post Type

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN post_type = 'photo' THEN 1 END) as photo_count,
    SUM(CASE WHEN post_type = 'photo' THEN likes_count ELSE 0 END) as photo_total_likes,
    ROUND(AVG(CASE WHEN post_type = 'photo' THEN likes_count END), 2) as photo_avg_likes,
    
    COUNT(CASE WHEN post_type = 'video' THEN 1 END) as video_count,
    SUM(CASE WHEN post_type = 'video' THEN likes_count ELSE 0 END) as video_total_likes,
    ROUND(AVG(CASE WHEN post_type = 'video' THEN likes_count END), 2) as video_avg_likes,
    
    COUNT(CASE WHEN post_type = 'carousel' THEN 1 END) as carousel_count,
    SUM(CASE WHEN post_type = 'carousel' THEN likes_count ELSE 0 END) as carousel_total_likes,
    ROUND(AVG(CASE WHEN post_type = 'carousel' THEN likes_count END), 2) as carousel_avg_likes
FROM posts;
```

**Explanation:**
- `COUNT(CASE WHEN...)` counts rows of each type
- `SUM(CASE WHEN... ELSE 0 END)` sums likes for each type
  - ELSE 0 prevents NULL (though not strictly necessary for SUM)
- `AVG(CASE WHEN...)` averages likes for each type
- All three post types in one row

**Key Learning:**
- One query replaces 3 separate queries (one per type)
- Shows all comparisons at once (photo vs video vs carousel)
- This pattern scales: add more types by adding more CASE statements

---

## Problem 3 Solution: Spotify - Premium vs Free User Streams

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN u.subscription_type = 'premium' THEN 1 END) as premium_stream_count,
    COUNT(CASE WHEN u.subscription_type = 'free' THEN 1 END) as free_stream_count
FROM streams s
INNER JOIN users u ON s.user_id = u.user_id;
```

**Explanation:**
- Joins streams to users to get subscription_type
- `COUNT(CASE WHEN subscription_type = 'premium' THEN 1 END)` counts premium streams
- `COUNT(CASE WHEN subscription_type = 'free' THEN 1 END)` counts free streams
- Single row output for easy comparison

**Key Learning:**
- Join to access filtering column (subscription_type)
- CASE WHEN filters before aggregation
- Shows business split: premium vs free (critical for revenue analysis)

---

## Problem 4 Solution: YouTube - Video Performance by Upload Status

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN upload_status = 'draft' THEN 1 END) as draft_count,
    COUNT(CASE WHEN upload_status = 'published' THEN 1 END) as published_count,
    COUNT(CASE WHEN upload_status = 'archived' THEN 1 END) as archived_count,
    
    SUM(CASE WHEN upload_status = 'draft' THEN views ELSE 0 END) as draft_views,
    SUM(CASE WHEN upload_status = 'published' THEN views ELSE 0 END) as published_views,
    SUM(CASE WHEN upload_status = 'archived' THEN views ELSE 0 END) as archived_views
FROM videos;
```

**Explanation:**
- COUNT() by status: how many videos in each state
- SUM() by status: total views for each state
- Example output:
  ```
  draft_count=10, published_count=100, archived_count=5
  draft_views=0, published_views=5000000, archived_views=50000
  ```

**Key Learning:**
- Mixing COUNT and SUM with CASE shows workflow status
- Drafts have no views (makes sense)
- Published has most views
- This is how editorial teams monitor content pipeline

---

## Problem 5 Solution: Amazon - Order Status Breakdown

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN order_status = 'pending' THEN 1 END) as pending_count,
    COUNT(CASE WHEN order_status = 'processing' THEN 1 END) as processing_count,
    COUNT(CASE WHEN order_status = 'shipped' THEN 1 END) as shipped_count,
    COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) as delivered_count,
    COUNT(CASE WHEN order_status = 'cancelled' THEN 1 END) as cancelled_count,
    
    SUM(CASE WHEN order_status = 'delivered' THEN order_amount ELSE 0 END) as delivered_revenue
FROM orders;
```

**Explanation:**
- COUNT() for each status (including cancelled)
- SUM() only for delivered orders (completed revenue)
- Example output:
  ```
  pending=50, processing=100, shipped=200, delivered=1000, cancelled=20
  delivered_revenue=$50,000,000
  ```

**Key Learning:**
- Count all statuses (to track progress)
- But only sum revenue for completed orders
- Shows full order pipeline + actual revenue
- Critical for order management dashboards

---

## Problem 6 Solution: Netflix - Account Metrics by Subscription Tier

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN subscription_tier = 'basic' THEN 1 END) as basic_count,
    COUNT(CASE WHEN subscription_tier = 'standard' THEN 1 END) as standard_count,
    COUNT(CASE WHEN subscription_tier = 'premium' THEN 1 END) as premium_count,
    
    ROUND(AVG(CASE WHEN subscription_tier = 'basic' THEN DATEDIFF(CURDATE(), signup_date) END), 1) as basic_avg_age,
    ROUND(AVG(CASE WHEN subscription_tier = 'standard' THEN DATEDIFF(CURDATE(), signup_date) END), 1) as standard_avg_age,
    ROUND(AVG(CASE WHEN subscription_tier = 'premium' THEN DATEDIFF(CURDATE(), signup_date) END), 1) as premium_avg_age
FROM users;
```

**Explanation:**
- COUNT(CASE WHEN...) counts users per tier
- AVG(CASE WHEN DATEDIFF(...)) averages account age per tier
- DATEDIFF(CURDATE(), signup_date) = account age in days
- Example output:
  ```
  basic_count=100,000, standard_count=50,000, premium_count=10,000
  basic_avg_age=180, standard_avg_age=300, premium_avg_age=500
  ```
- Premium users are older (more loyal)

**Key Learning:**
- Mix COUNT and AVG in same query
- DATEDIFF calculates age in days
- Shows tier distribution AND quality (average age = loyalty proxy)

---

## Problem 7 Solution: Uber - Completed vs Cancelled Rides

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN ride_status = 'completed' THEN 1 END) as completed_count,
    COUNT(CASE WHEN ride_status = 'cancelled_by_driver' THEN 1 END) as cancelled_by_driver_count,
    COUNT(CASE WHEN ride_status = 'cancelled_by_user' THEN 1 END) as cancelled_by_user_count,
    
    ROUND(AVG(CASE WHEN ride_status = 'completed' THEN driver_rating END), 2) as completed_avg_rating
FROM rides;
```

**Explanation:**
- COUNT() for all three statuses
- AVG() only for completed rides (only they have ratings)
- Example output:
  ```
  completed_count=5000, cancelled_by_driver_count=200, cancelled_by_user_count=100
  completed_avg_rating=4.85
  ```

**Key Learning:**
- Selective aggregation: sum/average only relevant rows
- Helps identify: cancellation rate, quality issues
- If high "cancelled_by_driver", maybe quality problem

---

## Problem 8 Solution: LinkedIn - Connection Types

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN connection_type = '1st degree' THEN 1 END) as first_degree_count,
    COUNT(CASE WHEN connection_type = '2nd degree' THEN 1 END) as second_degree_count,
    COUNT(CASE WHEN connection_type = '3rd degree' THEN 1 END) as third_degree_count,
    
    ROUND(AVG(CASE WHEN connection_type = '1st degree' THEN DATEDIFF(CURDATE(), connected_at) END), 0) as first_degree_avg_age,
    ROUND(AVG(CASE WHEN connection_type = '2nd degree' THEN DATEDIFF(CURDATE(), connected_at) END), 0) as second_degree_avg_age,
    ROUND(AVG(CASE WHEN connection_type = '3rd degree' THEN DATEDIFF(CURDATE(), connected_at) END), 0) as third_degree_avg_age
FROM connections;
```

**Explanation:**
- COUNT() shows network composition (how many of each type)
- AVG(DATEDIFF()) shows connection age by degree
- Example output:
  ```
  1st_degree_count=100, 2nd_degree_count=500, 3rd_degree_count=1000
  1st_degree_avg_age=365, 2nd_degree_avg_age=450, 3rd_degree_avg_age=180
  ```
- 1st degree older (closer relationships, older) vs 3rd (newer)

**Key Learning:**
- Shows network structure and relationship maturity
- Multiple conditions in one query

---

## Problem 9 Solution: Google Drive - File Statistics by Type

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN file_type = 'document' THEN 1 END) as document_count,
    COUNT(CASE WHEN file_type = 'spreadsheet' THEN 1 END) as spreadsheet_count,
    COUNT(CASE WHEN file_type = 'presentation' THEN 1 END) as presentation_count,
    COUNT(CASE WHEN file_type = 'other' THEN 1 END) as other_count,
    
    SUM(CASE WHEN file_type = 'document' THEN file_size_mb ELSE 0 END) as document_size,
    SUM(CASE WHEN file_type = 'spreadsheet' THEN file_size_mb ELSE 0 END) as spreadsheet_size,
    SUM(CASE WHEN file_type = 'presentation' THEN file_size_mb ELSE 0 END) as presentation_size,
    SUM(CASE WHEN file_type = 'other' THEN file_size_mb ELSE 0 END) as other_size
FROM files;
```

**Explanation:**
- COUNT() shows file counts by type
- SUM() shows total storage by type
- Example output:
  ```
  document_count=10000, spreadsheet_count=2000, presentation_count=500, other_count=3000
  document_size=5000MB, spreadsheet_size=20000MB, presentation_size=10000MB, other_size=8000MB
  ```
- Spreadsheets take most storage (data-heavy)

**Key Learning:**
- Helps understand storage usage patterns
- Which file types dominate? Where to optimize?

---

## Problem 10 Solution: WhatsApp - Message Statistics by Sender Type

**SQL Query:**
```sql
SELECT 
    COUNT(CASE WHEN sender_type = 'user' THEN 1 END) as user_message_count,
    COUNT(CASE WHEN sender_type = 'bot' THEN 1 END) as bot_message_count,
    COUNT(CASE WHEN sender_type = 'system' THEN 1 END) as system_message_count,
    
    SUM(CASE WHEN sender_type = 'user' THEN message_length ELSE 0 END) as user_total_chars,
    SUM(CASE WHEN sender_type = 'bot' THEN message_length ELSE 0 END) as bot_total_chars,
    SUM(CASE WHEN sender_type = 'system' THEN message_length ELSE 0 END) as system_total_chars
FROM messages;
```

**Explanation:**
- COUNT() by sender type (human, bot, system)
- SUM(message_length) shows total characters by type
- Example output:
  ```
  user_message_count=1000000, bot_message_count=50000, system_message_count=10000
  user_total_chars=100000000, bot_total_chars=500000, system_total_chars=100000
  ```

**Key Learning:**
- Shows message composition: mostly users (expected)
- Users send longest messages (natural language)
- Bots and systems send shorter messages (structured)

---

## Pattern 5 Summary: Key Takeaways

### CASE WHEN Syntax:

```sql
CASE 
    WHEN condition1 THEN value1
    WHEN condition2 THEN value2
    ELSE default_value
END
```

### Using with Aggregates:

```sql
COUNT(CASE WHEN condition THEN 1 END)           -- Count rows matching condition
SUM(CASE WHEN condition THEN amount ELSE 0 END) -- Sum for matching rows
AVG(CASE WHEN condition THEN value END)         -- Average for matching rows
MAX(CASE WHEN condition THEN value END)         -- Max for matching rows
```

### Common Patterns:

| Use Case | Pattern | Example |
|----------|---------|---------|
| Paid vs Free | COUNT(CASE WHEN is_paid THEN 1 END) | Segment users |
| Success vs Failure | SUM(CASE WHEN status='success' THEN 1 END) | Order completion rate |
| A/B Test Groups | COUNT(CASE WHEN variant='A' THEN 1 END) | Test comparisons |
| Multiple Metrics | Multiple CASE statements | All KPIs in one query |

### When to Use:

✅ Need multiple metrics in one query
✅ Want to compare segments (paid/free, A/B test, etc.)
✅ Need conditional sums (only count if condition met)
✅ Comparing multiple dimensions efficiently

### Performance Considerations:

- One query with CASE > multiple separate queries
- Scans table once instead of N times
- Better for dashboards and reporting
- Reduces database load significantly

### Common Mistakes:

❌ Forgetting ELSE 0 in SUM (can cause NULL issues)
❌ Using CASE in WHERE clause (use simple conditions instead)
❌ Not aliasing CASE expressions clearly
❌ Too many nested CASE statements (hard to read)
❌ Using CASE where simple filters work better

### Pro Tips:

✓ Always give CASE expressions clear aliases
✓ Use ROUND() for readability on averages
✓ Structure readably: one CASE per metric
✓ Combine with GROUP BY for per-segment analysis
✓ Test simple condition first, then add CASE

### Real-world Uses:

- **Dashboards**: All KPIs in one query
- **Reports**: Multiple metrics per segment
- **Analysis**: Comparing groups efficiently
- **Monitoring**: Different thresholds in one query
- **Churn Analysis**: At-risk vs healthy customers
- **Revenue**: Different customer segments

