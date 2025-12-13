# Pattern 3: Ranking with Window Functions - Practice Problems

## Understanding the Pattern

Window functions enable sophisticated ranking, comparison, and analysis without grouping data. This pattern is essential for finding top performers, detecting trends, and deep behavioral analytics.

**Key Concepts**:
- ROW_NUMBER: Unique sequential rank (1, 2, 3, 4...)
- RANK: Handles ties (1, 1, 3, 4...)
- DENSE_RANK: Continuous ranking with ties (1, 1, 2, 3...)
- LAG/LEAD: Compare with previous/next row
- NTILE: Divide into N percentiles
- FIRST_VALUE/LAST_VALUE: Get first/last value in partition
- Frame specifications: ROWS BETWEEN, RANGE BETWEEN

---

## Practice Problems

### Problem 1: YouTube - Top N Videos Per Creator (ROW_NUMBER)
**Difficulty**: Medium | **Real-world Use Case**: Creator Analytics Dashboard

You have tables:
- `channels` (channel_id, channel_name)
- `videos` (video_id, channel_id, title, published_date, views, watch_time_hours)

For each creator, rank their videos by views and find the top 3 most-watched videos. Include creator name, video title, views, and the rank. Order by creator name, then rank.

**Expected Output Columns**: channel_id, channel_name, video_rank, video_id, video_title, views, watch_time_hours

---

### Problem 2: Amazon - Percentile-Based Product Pricing (NTILE)
**Difficulty**: Medium | **Real-world Use Case**: Price Analytics & Market Segmentation

You have tables:
- `products` (product_id, product_name, category_id, price, inventory_count)
- `categories` (category_id, category_name)

Divide products within each category into 4 price quartiles (Q1: cheapest 25%, Q2, Q3, Q4: most expensive 25%). Include product name, price, category, and quartile assignment. Show only products in Q1 and Q4.

**Expected Output Columns**: category_id, category_name, product_id, product_name, price, price_quartile

---

### Problem 3: LinkedIn - Users' Job Title History Progression (LAG/LEAD)
**Difficulty**: Medium-High | **Real-world Use Case**: Career Trajectory Analysis

You have tables:
- `users` (user_id, user_name)
- `job_history` (history_id, user_id, job_title, company_name, start_date, end_date)

Track user career progression by showing their current job, previous job, and next job (if applicable). Include job titles and the salary level change (if salary data exists, otherwise just job title). Order by user and date.

**Expected Output Columns**: user_id, user_name, previous_job_title, current_job_title, current_company, next_job_title, current_start_date

---

### Problem 4: Instagram - Follower Growth Rank by Day
**Difficulty**: Medium-High | **Real-world Use Case**: Influencer Growth Tracking

You have tables:
- `daily_follower_stats` (stat_id, user_id, date, follower_count)
- `users` (user_id, user_name, account_type)

For users with "influencer" account type, calculate daily follower growth (difference from previous day) and rank them by growth rate. Show only top 10 accounts by today's growth. Include date, followers, growth, and growth rank.

**Expected Output Columns**: user_id, user_name, date, follower_count, daily_growth, daily_growth_rank

---

### Problem 5: Spotify - Listener Percentile by Streams (Dense Rank vs Rank)
**Difficulty**: Medium-High | **Real-world Use Case**: Artist Performance Tiers

You have tables:
- `artists` (artist_id, artist_name, genre)
- `artist_stats` (stat_id, artist_id, month, total_streams, listener_count)

For each genre, assign both RANK and DENSE_RANK to artists based on total streams. Show artists where rank and dense_rank differ (indicating ties). Include the difference and interpretation.

**Expected Output Columns**: artist_id, artist_name, genre, month, total_streams, rank_position, dense_rank_position, has_tie

---

### Problem 6: Netflix - Watch Pattern Analysis (FIRST_VALUE, LAST_VALUE)
**Difficulty**: High | **Real-world Use Case**: Content Performance & Viewing Behavior

You have tables:
- `watch_history` (watch_id, user_id, content_id, watched_date, duration_minutes, completed)

For each user's watching pattern in the last 30 days, find: first content watched, last content watched, total watch hours, and what percentage of their watch time came from the first watched content. Order by user_id.

**Expected Output Columns**: user_id, first_content_watched, first_watch_date, last_content_watched, last_watch_date, total_watch_hours, first_content_percentage

---

### Problem 7: Uber - Driver Earnings Ranking with Moving Comparison (LAG)
**Difficulty**: High | **Real-world Use Case**: Driver Earning Trends & Performance

You have tables:
- `drivers` (driver_id, driver_name)
- `daily_earnings` (earning_id, driver_id, date, earnings, rides_completed)

For each driver, calculate week-over-week earnings change. Show driver name, current week earnings, previous week earnings, week-over-week change %, and rank them by change. Show top 20 drivers by positive growth.

**Expected Output Columns**: driver_id, driver_name, current_week_earnings, previous_week_earnings, wow_change_percentage, growth_rank

---

### Problem 8: Google Drive - Storage Usage Trend Analysis (FIRST_VALUE, LAG)
**Difficulty**: High | **Real-world Use Case**: Storage Management & Usage Trends

You have tables:
- `monthly_storage_stats` (stat_id, user_id, month, storage_used_bytes)
- `users` (user_id, user_name, plan_type)

Track each user's storage trend: initial storage (first month), current storage, month-over-month change, and acceleration (is growth rate increasing?). Show users whose storage is growing. Include plan type.

**Expected Output Columns**: user_id, user_name, plan_type, initial_storage_gb, current_storage_gb, total_growth_gb, growth_rate_acceleration, months_of_growth

---

### Problem 9: Twitter - Trending Topic Rank Dynamics (RANK vs Dense Rank)
**Difficulty**: High | **Real-world Use Case**: Real-time Trending Analytics

You have tables:
- `hourly_trend_stats` (stat_id, hashtag_id, hour, tweet_count, unique_users, engagement_score)
- `hashtags` (hashtag_id, hashtag_name)

For each hour of the last 7 days, rank hashtags by engagement score. Show hashtags that achieved different ranks at different hours and the range of positions they held. Identify the most volatile hashtags.

**Expected Output Columns**: hashtag_id, hashtag_name, min_rank, max_rank, rank_volatility, appearances_in_top_10, trending_hours

---

### Problem 10: LinkedIn - Skill Endorsement Momentum (Multiple Window Functions)
**Difficulty**: High | **Real-world Use Case**: Skill Demand & Talent Market Dynamics

You have tables:
- `skill_stats` (stat_id, skill_id, week, endorsement_count, unique_endorsers)
- `skills` (skill_id, skill_name, category)

For each skill, calculate: weekly ranking by endorsements, week-over-week change, cumulative endorsements, and identify skills with accelerating demand (each week higher than previous). Show top 20 skills by current rank with momentum analysis.

**Expected Output Columns**: skill_id, skill_name, category, current_week, endorsements_this_week, week_rank, wow_change, cumulative_endorsements, weeks_of_growth, momentum_score
