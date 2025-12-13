# Pattern 5: Conditional Aggregations (CASE WHEN) - Practice Problems

## Understanding the Pattern

Conditional aggregations enable computing multiple metrics in a single efficient query by using CASE statements within aggregate functions. This pattern demonstrates understanding of data segmentation and categorical analysis.

**Key Concepts**:
- CASE WHEN syntax for conditional logic
- Nested CASE statements
- CASE within aggregate functions: SUM(CASE WHEN...), COUNT(CASE WHEN...), AVG(CASE WHEN...)
- Multiple conditional branches for segmentation
- NULL handling in conditional logic
- Computing derived metrics (rates, percentages, ratios)

---

## Practice Problems

### Problem 1: Amazon - Revenue Split by Order Status
**Difficulty**: Medium | **Real-world Use Case**: Order Analytics & Financial Reporting

You have tables:
- `orders` (order_id, order_date, total_amount, order_status)
- `order_items` (order_item_id, order_id, quantity, unit_price)

For each day in the last 30 days, calculate: total orders, revenue from completed orders, revenue from cancelled orders, revenue from pending orders, and cancellation rate. Show only days with 100+ orders.

**Expected Output Columns**: order_date, total_orders, completed_revenue, cancelled_revenue, pending_revenue, cancellation_rate

---

### Problem 2: Netflix - Subscriber Segmentation Metrics
**Difficulty**: Medium | **Real-world Use Case**: Subscription & Revenue Analytics

You have tables:
- `users` (user_id, subscription_tier, signup_date, churn_date)
- `billing` (billing_id, user_id, billing_date, amount_paid, status)

For the last 30 days, calculate: total active subscribers by tier, monthly revenue by tier, trial conversion rate (trial users who converted to paid), and churn rate by tier.

**Expected Output Columns**: period_month, basic_active_users, standard_active_users, premium_active_users, basic_revenue, standard_revenue, premium_revenue, trial_conversion_rate, churn_rate_by_tier

---

### Problem 3: Uber - Ride Outcome Distribution
**Difficulty**: Medium-High | **Real-world Use Case**: Operations & Driver Performance

You have tables:
- `ride_requests` (request_id, driver_id, request_time, outcome)
- `rides` (ride_id, request_id, ride_distance_km, ride_fare, completion_time_minutes)

For each driver in the last 90 days, calculate: total ride requests, accepted rides count, cancelled rides count, completed rides count, acceptance rate, cancellation rate, and average fare for completed rides only.

**Expected Output Columns**: driver_id, total_requests, accepted_count, cancelled_count, completed_count, acceptance_rate, cancellation_rate, avg_completed_fare

---

### Problem 4: Instagram - Content Performance by Type
**Difficulty**: Medium-High | **Real-world Use Case**: Content Analytics & Recommendation

You have tables:
- `posts` (post_id, user_id, created_at, post_type, likes, comments, shares)
- `post_type`: [photo, video, carousel, reel]

For users with 10k+ followers, compare performance metrics by post type: average likes, average comments, average shares, engagement rate (total engagement / total posts). Show only post types posted at least 5 times per user.

**Expected Output Columns**: user_id, post_type, count_posts, avg_likes, avg_comments, avg_shares, engagement_rate

---

### Problem 5: Twitter - Tweet Engagement by Tweet Type
**Difficulty**: Medium-High | **Real-world Use Case**: Content Performance & Strategy

You have tables:
- `tweets` (tweet_id, user_id, created_at, text, likes, retweets, replies, has_link, has_image, has_video)

Calculate engagement metrics: total tweets, tweets with links, tweets with images, tweets with video, engagement rate (total engagement / total tweets), and engagement rate by content type. Show metrics for the last 30 days.

**Expected Output Columns**: period, total_tweets, tweet_with_link_count, tweet_with_image_count, tweet_with_video_count, overall_engagement_rate, link_tweet_engagement_rate, image_tweet_engagement_rate, video_tweet_engagement_rate

---

### Problem 6: Spotify - Premium vs Free User Metrics
**Difficulty**: High | **Real-world Use Case**: Conversion & Monetization Analytics

You have tables:
- `users` (user_id, user_type, signup_date, premium_start_date)
- `streams` (stream_id, user_id, stream_date, duration_minutes, track_id)

Calculate monthly: free user streams, premium user streams, average streams per free user, average streams per premium user, free-to-premium conversion rate, and premium churn rate.

**Expected Output Columns**: year_month, free_user_count, premium_user_count, free_total_streams, premium_total_streams, avg_streams_per_free_user, avg_streams_per_premium_user, free_to_premium_conversions, churn_count

---

### Problem 7: YouTube - Content Performance by Channel Type
**Difficulty**: High | **Real-world Use Case**: Creator Program Analytics

You have tables:
- `channels` (channel_id, channel_type, partner_status, created_date)
- `videos` (video_id, channel_id, published_date, views, watch_time_hours, likes, dislikes)
- `channel_type`: [individual, brand, news, gaming, education, entertainment]

For channels active in last 180 days, calculate: total videos, average views per video, watch-through rate (watch_time / view_count), like-to-dislike ratio, and metrics broken down by channel type.

**Expected Output Columns**: channel_type, count_channels, total_videos, avg_views_per_video, avg_watch_through_rate, avg_like_to_dislike_ratio

---

### Problem 8: LinkedIn - User Engagement by Profile Completeness
**Difficulty**: High | **Real-world Use Case**: User Quality & Platform Health

You have tables:
- `users` (user_id, has_profile_photo, has_job_title, has_industry, has_skills, profile_completion_percentage)
- `user_activity` (activity_id, user_id, activity_date, action_type)
- `action_type`: [connection_request, profile_view, post_engagement, job_application, message_sent]

Calculate: user count by profile completion quartiles, engagement rate by quartile, connection request rate, post engagement rate, and message activity rate. Identify which profile elements drive engagement.

**Expected Output Columns**: profile_completion_quartile, user_count, engagement_rate, avg_connection_requests, avg_post_engagements, avg_messages_sent, profile_elements_completed

---

### Problem 9: Amazon - Purchase Behavior Segmentation
**Difficulty**: High | **Real-world Use Case**: Customer Analytics & Marketing

You have tables:
- `customers` (customer_id, signup_date)
- `orders` (order_id, customer_id, order_date, total_amount, status)
- `order_items` (order_item_id, order_id, product_id, category, unit_price, quantity)

Segment customers by purchase patterns: calculate for each customer their lifetime value, count of completed vs cancelled orders, average order value, product category diversity, and segment them as (high-value, medium-value, low-value) and (high-frequency, low-frequency).

**Expected Output Columns**: customer_id, lifetime_value, completed_orders, cancelled_orders, order_cancellation_rate, avg_order_value, unique_product_categories, value_segment, frequency_segment

---

### Problem 10: WhatsApp - Message Quality & Engagement Metrics
**Difficulty**: High | **Real-world Use Case**: Communication Patterns & Engagement

You have tables:
- `messages` (message_id, sender_id, chat_id, sent_timestamp, message_text, has_emoji, has_media, has_link, read_count, reaction_count)
- `chats` (chat_id, chat_type, member_count)
- `chat_type`: [one_on_one, group]

For the last 30 days, calculate by chat type: average message length, percentage with emoji, percentage with media, percentage with links, average reactions per message, and engagement metrics. Identify high-engagement vs low-engagement communication patterns.

**Expected Output Columns**: chat_type, total_messages, avg_message_length, percent_with_emoji, percent_with_media, percent_with_link, avg_reactions_per_message, avg_read_count, engagement_level
