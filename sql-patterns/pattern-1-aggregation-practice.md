# Pattern 1: Aggregation with GROUP BY - Practice Problems

## Understanding the Pattern

Aggregation with GROUP BY is used to calculate metrics and KPIs across different dimensions. This is the most common pattern you'll encounter in real product analytics and data dashboards.

**Key Concepts**:
- Basic aggregates: COUNT, SUM, AVG, MIN, MAX
- Multi-level grouping (by user, time period, category)
- HAVING clause for filtering aggregated results
- Handling NULL values and edge cases

---

## Practice Problems

### Problem 1: Twitter - Daily Tweet Activity Metrics
**Difficulty**: Medium | **Real-world Use Case**: Twitter Analytics Dashboard

You have a `tweets` table with columns: tweet_id, user_id, created_at, likes_count, retweets_count, tweet_text.

Write a query to find for each day, the total number of tweets posted, total likes received, and average retweets per tweet. Only include days with more than 100 tweets posted. Order by date descending.

**Expected Output Columns**: date, total_tweets, total_likes, avg_retweets_per_tweet

---

### Problem 2: YouTube - Channel Performance Ranking
**Difficulty**: Medium | **Real-world Use Case**: Content Creator Analytics

You have tables:
- `channels` (channel_id, channel_name, created_at)
- `videos` (video_id, channel_id, published_at, views, watch_time_hours)

For each channel, calculate: total videos published, total views, total watch hours, and average watch time per video. Rank channels by total watch hours and show only top 10 channels by watch hours.

**Expected Output Columns**: channel_id, channel_name, total_videos, total_views, total_watch_hours, avg_watch_hours_per_video, watch_hours_rank

---

### Problem 3: Spotify - Artist Revenue Breakdown by Country
**Difficulty**: Medium-High | **Real-world Use Case**: Artist Compensation Analytics

You have tables:
- `streams` (stream_id, track_id, artist_id, country, stream_date, revenue_generated)
- `artists` (artist_id, artist_name, genre)

Calculate for each artist and country combination: total streams, total revenue, and average revenue per stream. Show only combinations with more than 10,000 total streams. Include artist name and genre. Order by artist name, then revenue descending.

**Expected Output Columns**: artist_id, artist_name, genre, country, total_streams, total_revenue, avg_revenue_per_stream

---

### Problem 4: Instagram - Hashtag Performance Analysis
**Difficulty**: Medium | **Real-world Use Case**: Social Media Analytics

You have tables:
- `posts` (post_id, user_id, created_at, likes, comments, shares)
- `post_hashtags` (post_id, hashtag_id)
- `hashtags` (hashtag_id, hashtag_name)

For each hashtag used more than 500 times, calculate: number of posts using it, average likes per post, average comments per post, total engagement (likes + comments + shares). Order by total engagement descending. Limit to top 20.

**Expected Output Columns**: hashtag_id, hashtag_name, post_count, avg_likes, avg_comments, avg_shares, total_engagement

---

### Problem 5: Amazon - Product Category Sales Summary
**Difficulty**: Medium-High | **Real-world Use Case**: Marketplace Performance Dashboard

You have tables:
- `products` (product_id, product_name, category_id, subcategory_id, price)
- `orders` (order_id, product_id, order_date, quantity, discount_percentage)

For each category and subcategory, calculate: number of products sold, total revenue (quantity × price × (1 - discount_percentage)), average order value, and number of distinct customers. Show only categories with total revenue > $100,000. Order by total revenue descending.

**Expected Output Columns**: category_id, subcategory_id, product_count, total_revenue, avg_order_value, distinct_customers

---

### Problem 6: Netflix - User Engagement by Device Type
**Difficulty**: Medium-High | **Real-world Use Case**: Platform Analytics

You have tables:
- `watch_history` (watch_id, user_id, device_type, watch_date, duration_minutes, content_id, completed)
- `users` (user_id, subscription_tier, signup_date)

For each device type and subscription tier, calculate: active users (users with at least 1 watch event), total watch hours, average watch duration, completion rate (completed / total watches). Filter for watch dates in the last 30 days. Show only combinations with 100+ active users. Order by total watch hours descending.

**Expected Output Columns**: device_type, subscription_tier, active_users, total_watch_hours, avg_watch_duration, completion_rate

---

### Problem 7: Uber - Driver Performance Metrics
**Difficulty**: Medium-High | **Real-world Use Case**: Driver Incentive Program Analytics

You have tables:
- `rides` (ride_id, driver_id, ride_date, ride_time_minutes, distance_km, fare_amount, rating)
- `drivers` (driver_id, driver_name, vehicle_type, signup_date)

Calculate for each driver in the last 90 days: total rides, average ride duration, average distance, total earnings, average rating. Include vehicle type. Show only drivers with 50+ rides. Order by average rating descending, then total earnings descending.

**Expected Output Columns**: driver_id, driver_name, vehicle_type, total_rides, avg_duration_minutes, avg_distance_km, total_earnings, avg_rating

---

### Problem 8: Google Drive - Storage Usage by File Type and User
**Difficulty**: High | **Real-world Use Case**: Storage Analytics & Quota Management

You have tables:
- `files` (file_id, user_id, file_type, size_bytes, created_date, last_accessed_date)
- `users` (user_id, user_email, plan_type, storage_limit_bytes)

For each user and file_type combination, calculate: file count, total storage used, average file size, days since last access (average). Show only combinations where total storage > 1GB. Include user plan type. Order by total storage descending.

**Expected Output Columns**: user_id, user_email, plan_type, file_type, file_count, total_storage_gb, avg_file_size_mb, avg_days_since_access

---

### Problem 9: WhatsApp - Message Volume by Chat Type and Hour
**Difficulty**: High | **Real-world Use Case**: Real-time Communication Analytics

You have tables:
- `messages` (message_id, sender_id, chat_id, chat_type, sent_timestamp, message_length, has_media)
- `chats` (chat_id, chat_type, member_count, created_date)

Calculate hourly statistics for the last 7 days: for each hour and chat type, count total messages, total messages with media, average message length. Show only hours with 1000+ total messages. Order by timestamp descending, then message count descending.

**Expected Output Columns**: hour_of_day, chat_type, total_messages, messages_with_media, avg_message_length

---

### Problem 10: LinkedIn - Skills Endorsement Distribution
**Difficulty**: High | **Real-world Use Case**: Skill Analytics & Talent Marketplace

You have tables:
- `users` (user_id, user_name, job_title)
- `endorsements` (endorsement_id, user_id, skill_id, endorser_id, endorsed_date)
- `skills` (skill_id, skill_name, category)

For each skill in the "Software Development" category, calculate for the last 90 days: total endorsements received, number of users endorsed with this skill, average endorsements per user, and endorsement growth rate (endorsements this month vs previous month). Show skills with 100+ total endorsements. Order by total endorsements descending.

**Expected Output Columns**: skill_id, skill_name, total_endorsements, users_endorsed, avg_endorsements_per_user, endorsement_growth_percentage
