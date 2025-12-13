# Pattern 5: Conditional Aggregation with CASE WHEN - Easy Practice Problems

## Understanding the Pattern

Conditional aggregation lets you compute multiple metrics in a single query using CASE WHEN. Instead of 5 separate queries, you can get 5 metrics at once. This is more efficient and lets you make comparisons easily.

**Key Concepts You'll Learn**:
- CASE WHEN syntax (IF/THEN logic in SQL)
- CASE within aggregate functions (SUM, COUNT, AVG)
- Multiple conditional branches
- NULL handling
- Common real-world use cases: paid vs free, new vs returning, A/B tests

**Why This Matters**: This demonstrates you can write efficient queries. Instead of joining data multiple times, smart CASE WHEN reduces queries and improves performance.

---

## Practice Problems

### Problem 1: Twitter - Tweet Engagement by Type

**Difficulty**: Easy | **Real-world Use Case**: Content Analytics

You have a `tweets` table with columns: `tweet_id`, `created_date`, `likes_count`, `is_retweet` (0 or 1).

For today's date, count how many tweets are original tweets vs retweets. Show total_original_tweets and total_retweets in a single row.

**Expected Output**: total_original_tweets, total_retweets

---

### Problem 2: Instagram - Post Engagement by Post Type

**Difficulty**: Easy | **Real-world Use Case**: Content Performance

You have a `posts` table with columns: `post_id`, `post_type` ('photo', 'video', 'carousel'), `likes_count`.

Show statistics for each post type: count of posts, total likes, and average likes. Include all post types in ONE row.

**Expected Output**: photo_count, photo_total_likes, photo_avg_likes, video_count, video_total_likes, video_avg_likes, carousel_count, carousel_total_likes, carousel_avg_likes

---

### Problem 3: Spotify - Premium vs Free User Streams

**Difficulty**: Easy | **Real-world Use Case**: Revenue Analytics

You have tables:
- `users` (user_id, subscription_type: 'premium' or 'free')
- `streams` (stream_id, user_id, stream_date)

Count total streams for premium users vs free users. Show both in one row.

**Expected Output**: premium_stream_count, free_stream_count

---

### Problem 4: YouTube - Video Performance by Upload Status

**Difficulty**: Easy-Medium | **Real-world Use Case**: Video Management

You have a `videos` table with columns: `video_id`, `upload_status` ('draft', 'published', 'archived'), `views`, `published_at`.

Show count of videos in each status and total views per status. Include all statuses in one row.

**Expected Output**: draft_count, published_count, archived_count, draft_views, published_views, archived_views

---

### Problem 5: Amazon - Order Status Breakdown

**Difficulty**: Easy-Medium | **Real-world Use Case**: Order Management

You have an `orders` table with columns: `order_id`, `order_status` ('pending', 'processing', 'shipped', 'delivered', 'cancelled'), `order_amount`.

Show count of orders in each status and total revenue per status (only for successful orders: delivered). 

**Expected Output**: pending_count, processing_count, shipped_count, delivered_count, cancelled_count, delivered_revenue

---

### Problem 6: Netflix - Account Metrics by Subscription Tier

**Difficulty**: Easy-Medium | **Real-world Use Case**: Subscriber Analytics

You have a `users` table with columns: `user_id`, `subscription_tier` ('basic', 'standard', 'premium'), `signup_date`.

Count users per tier and show average account age for each tier.

**Expected Output**: basic_count, standard_count, premium_count, basic_avg_age, standard_avg_age, premium_avg_age

---

### Problem 7: Uber - Completed vs Cancelled Rides

**Difficulty**: Easy-Medium | **Real-world Use Case**: Ride Quality Metrics

You have a `rides` table with columns: `ride_id`, `ride_status` ('completed', 'cancelled_by_driver', 'cancelled_by_user'), `fare_amount`, `driver_rating`.

Show count of each status type and average rating (only for completed rides). Include all statuses.

**Expected Output**: completed_count, cancelled_by_driver_count, cancelled_by_user_count, completed_avg_rating

---

### Problem 8: LinkedIn - Connection Types

**Difficulty**: Easy-Medium | **Real-world Use Case**: Network Analytics

You have a `connections` table with columns: `connection_id`, `connection_type` ('1st degree', '2nd degree', '3rd degree'), `connected_at`.

Count connections by type and show average connection age for each type.

**Expected Output**: first_degree_count, second_degree_count, third_degree_count, first_degree_avg_age, second_degree_avg_age, third_degree_avg_age

---

### Problem 9: Google Drive - File Statistics by Type

**Difficulty**: Easy-Medium | **Real-world Use Case**: Storage Analytics

You have a `files` table with columns: `file_id`, `file_type` ('document', 'spreadsheet', 'presentation', 'other'), `file_size_mb`, `owner_id`.

Show count of files by type and total size by type.

**Expected Output**: document_count, spreadsheet_count, presentation_count, other_count, document_size, spreadsheet_size, presentation_size, other_size

---

### Problem 10: WhatsApp - Message Statistics by Sender Type

**Difficulty**: Easy-Medium | **Real-world Use Case**: Chat Analytics

You have a `messages` table with columns: `message_id`, `conversation_id`, `sender_type` ('user', 'bot', 'system'), `sent_at`, `message_length`.

Count messages by sender type and show total characters sent by type.

**Expected Output**: user_message_count, bot_message_count, system_message_count, user_total_chars, bot_total_chars, system_total_chars

