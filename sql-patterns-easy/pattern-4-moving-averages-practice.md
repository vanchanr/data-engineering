# Pattern 4: Moving Averages & Cumulative Sums - Easy Practice Problems

## Understanding the Pattern

Time-series analysis is critical for product analytics. Moving averages smooth out noise, and cumulative sums track progress over time. These patterns help answer: "What's the trend?" and "How much total so far?"

**Key Concepts You'll Learn**:
- Cumulative sums (running total)
- Moving averages (rolling windows)
- Date-based ordering and grouping
- Window function frame clauses (ROWS BETWEEN)
- Handling missing dates/gaps

**Why This Matters**: Product dashboards need to show trends. Moving averages prevent single-day spikes from misleading leaders.

---

## Practice Problems

### Problem 1: Twitter - Cumulative Tweet Count Per User

**Difficulty**: Easy | **Real-world Use Case**: User Activity Tracking

You have a `tweets` table with columns: `tweet_id`, `user_id`, `created_date`.

For user_id = 1, show their tweets over time with a cumulative count. Include created_date, tweet_count (for that day), and cumulative_tweets. Order by date.

**Expected Output**: created_date, daily_tweet_count, cumulative_tweets

---

### Problem 2: Instagram - 7-Day Rolling Average of Likes

**Difficulty**: Easy-Medium | **Real-world Use Case**: Post Performance Trends

You have a `daily_metrics` table with columns: `metric_date`, `total_likes`.

Calculate a 7-day moving average of likes. For each date, show total_likes and the 7-day rolling average. Include dates with less than 7 days of history (average only the available days). Order by date.

**Expected Output**: metric_date, total_likes, moving_avg_7day

---

### Problem 3: Spotify - Cumulative Streams Per Artist (Monthly)

**Difficulty**: Easy-Medium | **Real-world Use Case**: Artist Growth Tracking

You have a `monthly_streams` table with columns: `artist_id`, `month`, `stream_count`.

For artist_id = 5, show their streams by month with cumulative total. Include month, monthly_streams, and cumulative_streams. Order by month.

**Expected Output**: month, monthly_streams, cumulative_streams

---

### Problem 4: YouTube - 30-Day Rolling Average Views Per Channel

**Difficulty**: Easy-Medium | **Real-world Use Case**: Channel Performance Monitoring

You have a `daily_channel_views` table with columns: `channel_id`, `view_date`, `view_count`.

Calculate 30-day rolling average views. For channel_id = 10, show each date's views and the 30-day moving average. Order by date descending and limit to last 60 days.

**Expected Output**: view_date, view_count, moving_avg_30day

---

### Problem 5: Amazon - Cumulative Revenue Per Day

**Difficulty**: Easy-Medium | **Real-world Use Case**: Revenue Tracking Dashboard

You have a `orders` table with columns: `order_date`, `order_amount`.

Show cumulative revenue by day. Include order_date, daily_revenue (total for that day), and cumulative_revenue (total so far). Order by date. Limit to last 30 days.

**Expected Output**: order_date, daily_revenue, cumulative_revenue

---

### Problem 6: Netflix - 14-Day Rolling Average Watch Hours

**Difficulty**: Easy-Medium | **Real-world Use Case**: Subscriber Engagement Trends

You have a `daily_watch_hours` table with columns: `watch_date`, `total_watch_hours`.

Calculate 14-day rolling average. For each date, show daily total and 14-day moving average. Order by date descending and show last 90 days.

**Expected Output**: watch_date, daily_hours, moving_avg_14day

---

### Problem 7: Uber - Cumulative Rides Per Driver (Daily)

**Difficulty**: Easy-Medium | **Real-world Use Case**: Driver Productivity Tracking

You have a `rides` table with columns: `driver_id`, `ride_date`, `ride_count` (aggregated per driver per day).

For driver_id = 42, show their daily rides with cumulative total. Include ride_date, daily_rides, and cumulative_rides. Order by date.

**Expected Output**: ride_date, daily_rides, cumulative_rides

---

### Problem 8: LinkedIn - 30-Day Cumulative Endorsements Per Skill

**Difficulty**: Easy-Medium | **Real-world Use Case**: Skill Popularity Growth

You have an `endorsements` table with columns: `skill_id`, `endorsement_date`, `endorsement_count` (daily count).

For skill_id = 123, show daily endorsements with cumulative total over the last 30 days. Include endorsement_date, daily_count, and cumulative_count. Order by date.

**Expected Output**: endorsement_date, daily_count, cumulative_count

---

### Problem 9: Google Drive - 7-Day Rolling Average File Uploads Per User

**Difficulty**: Easy-Medium | **Real-world Use Case**: User Activity Monitoring

You have a `file_uploads` table with columns: `user_id`, `upload_date`, `upload_count` (daily count).

For user_id = 99, show daily uploads with 7-day moving average. Include upload_date, daily_uploads, and moving_avg_7day. Order by date and show last 30 days.

**Expected Output**: upload_date, daily_uploads, moving_avg_7day

---

### Problem 10: WhatsApp - Cumulative Messages Per Conversation (Weekly)

**Difficulty**: Easy-Medium | **Real-world Use Case**: Conversation Growth Analysis

You have a `weekly_messages` table with columns: `conversation_id`, `week_start_date`, `message_count`.

For conversation_id = 'CONV_001', show weekly messages with cumulative total. Include week_start_date, weekly_messages, and cumulative_messages. Order by date.

**Expected Output**: week_start_date, weekly_messages, cumulative_messages

