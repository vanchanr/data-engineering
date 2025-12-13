# Pattern 4: Moving Averages & Cumulative Sums - Practice Problems

## Understanding the Pattern

Time-series analysis is critical for understanding trends, growth patterns, and performance changes. Moving averages smooth volatility, while cumulative sums show cumulative impact over time.

**Key Concepts**:
- Moving averages (7-day, 30-day rolling windows)
- Cumulative sums and counts
- Handling date gaps and incomplete time series
- Window frame clauses: ROWS BETWEEN, RANGE BETWEEN
- Performance optimization for large datasets
- Year-over-year and month-over-month comparisons

---

## Practice Problems

### Problem 1: Spotify - 7-Day Rolling Average Stream Count
**Difficulty**: Medium | **Real-world Use Case**: Music Streaming Analytics Dashboard

You have tables:
- `daily_streams` (date, artist_id, stream_count)
- `artists` (artist_id, artist_name)

For each artist, calculate a 7-day rolling average of streams for the last 90 days. Show artist name, date, daily streams, 7-day moving average, and trend (up/down/flat). Order by artist name and date.

**Expected Output Columns**: artist_id, artist_name, date, daily_streams, 7day_rolling_avg, trend_vs_previous_week

---

### Problem 2: Netflix - 30-Day Cumulative Watch Hours
**Difficulty**: Medium | **Real-world Use Case**: Subscriber Engagement Tracking

You have tables:
- `watch_history` (watch_id, user_id, watched_date, duration_minutes, content_type)
- `users` (user_id, signup_date)

For users who signed up in the last 90 days, show their cumulative watch hours on a daily basis for the first 30 days of their subscription. Include watch hours that day and cumulative total. Order by user_id and date.

**Expected Output Columns**: user_id, date, day_of_subscription, daily_watch_hours, cumulative_watch_hours, signup_date

---

### Problem 3: YouTube - 14-Day Moving Average Video Views with Gap Handling
**Difficulty**: Medium-High | **Real-world Use Case**: Content Performance Trends

You have tables:
- `video_daily_stats` (date, video_id, views, watch_time_hours)
- `videos` (video_id, channel_id, title, publish_date)

For videos published more than 30 days ago, calculate a 14-day moving average of views. Handle gaps where no data exists (fill with previous day's data or average). Show date, views, moving average, and identify anomaly days (views > 2x moving average).

**Expected Output Columns**: video_id, title, date, views, 14day_moving_avg, is_anomaly_day, anomaly_factor

---

### Problem 4: Amazon - Cumulative Revenue and Running Sales Count
**Difficulty**: Medium-High | **Real-world Use Case**: Sales Dashboard & Revenue Tracking

You have tables:
- `orders` (order_id, order_date, total_amount, status)
- `order_items` (order_item_id, order_id, product_id, quantity, unit_price)

For each day in the last 180 days, calculate: daily revenue, cumulative revenue since start of period, cumulative order count, and the percentage of period's total revenue accumulated so far. Filter only completed orders.

**Expected Output Columns**: order_date, daily_revenue, cumulative_revenue, cumulative_order_count, percent_of_total_revenue_accrued

---

### Problem 5: Twitter - Tweet Volume Trends (30-Day MA with Month-over-Month)
**Difficulty**: High | **Real-world Use Case**: Platform Activity Analytics

You have tables:
- `tweets` (tweet_id, user_id, created_at, engagement_score)

Calculate a 30-day moving average of daily tweet volume for the last 180 days. Compare each month's average with the same month last year (year-over-year). Identify months with significant YoY changes (>10%).

**Expected Output Columns**: date, daily_tweet_count, 30day_moving_avg, month, current_month_avg, prior_year_month_avg, yoy_change_percentage, is_significant_change

---

### Problem 6: Uber - Cumulative Earnings with Running Statistics
**Difficulty**: High | **Real-world Use Case**: Driver Earnings Analytics

You have tables:
- `daily_driver_stats` (date, driver_id, earnings, rides_completed, hours_online)
- `drivers` (driver_id, driver_name)

For each driver in the last 90 days, show: date, earnings, cumulative earnings in period, 7-day moving average earnings, and running average earnings per hour. Identify days where earnings exceeded the driver's 30-day average.

**Expected Output Columns**: driver_id, driver_name, date, daily_earnings, cumulative_earnings, 7day_moving_avg_earnings, running_avg_earnings_per_hour, vs_30day_average

---

### Problem 7: Google Drive - Monthly Storage Growth Tracking
**Difficulty**: High | **Real-world Use Case**: Storage Quota & Capacity Planning

You have tables:
- `monthly_storage_snapshots` (snapshot_date, user_id, storage_used_bytes, file_count)

For users with plans over 100GB, track monthly storage growth: current month storage, previous month storage, month-over-month growth %, and cumulative growth in past 6 months. Identify users approaching their storage limits (80%+ used).

**Expected Output Columns**: user_id, snapshot_month, storage_gb, previous_month_storage_gb, mom_growth_percent, cumulative_6month_growth_percent, percent_of_limit_used, approaching_limit

---

### Problem 8: LinkedIn - Connection Growth Trajectory (Accelerating/Decelerating)
**Difficulty**: High | **Real-world Use Case**: User Growth Analytics

You have tables:
- `daily_connection_stats` (date, user_id, new_connections, total_connections)

For users who gained connections in the last 60 days, show: date, new connections that day, cumulative new connections, 7-day moving average of new connections, and acceleration (is growth rate increasing week-over-week?).

**Expected Output Columns**: user_id, date, new_connections_today, cumulative_new_connections, 7day_moving_avg_new_connections, weekly_acceleration_trend

---

### Problem 9: Spotify - Artist Streaming Momentum (Cumulative with Trend)
**Difficulty**: High | **Real-world Use Case**: Artist Growth & Chart Performance

You have tables:
- `daily_artist_streams` (date, artist_id, stream_count)
- `artists` (artist_id, artist_name, genre, release_date)

For artists with album releases, track streams from release date: daily streams, cumulative streams, 14-day moving average, and identify breakout days (streams exceed 2-week average). Show data for 30 days post-release.

**Expected Output Columns**: artist_id, artist_name, release_date, days_since_release, daily_streams, cumulative_streams, 14day_moving_avg, is_breakout_day, momentum_score

---

### Problem 10: Netflix - Content Lifecycle Analysis (Complete Time Series)
**Difficulty**: High | **Real-world Use Case**: Content Performance Lifecycle

You have tables:
- `content_daily_stats` (date, content_id, views, watch_time_hours)
- `content` (content_id, content_type, release_date)

For each content, analyze its lifecycle: daily views from release, 7-day moving average, cumulative views, and life cycle phase (launch week: explosive growth, mature: stable, decline: below recent average). Show full 90-day trajectory.

**Expected Output Columns**: content_id, content_type, release_date, days_since_release, daily_views, 7day_moving_avg, cumulative_views, lifecycle_phase, phase_indicator
