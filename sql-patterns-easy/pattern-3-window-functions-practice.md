# Pattern 3: Ranking with Window Functions - Easy Practice Problems

## Understanding the Pattern

Window functions let you rank, compare, and analyze data while keeping individual rows intact (unlike GROUP BY which combines rows). Think of it as: "for each row, show me where it ranks compared to others."

**Key Concepts You'll Learn**:
- ROW_NUMBER: Assigns sequential ranks (1, 2, 3, 4...)
- RANK: Handles ties by repeating rank (1, 2, 2, 4...)
- DENSE_RANK: Handles ties without gaps (1, 2, 2, 3...)
- PARTITION BY: Divide data into groups
- ORDER BY: Sort within each group/window
- Top N items per category pattern

**Why This Matters**: Window functions appear in almost every FAANG interview. They're essential for "top N per category" problems.

---

## Practice Problems

### Problem 1: Twitter - Top 3 Most Liked Tweets Per User

**Difficulty**: Easy | **Real-world Use Case**: User Best Content

You have a `tweets` table with columns: `tweet_id`, `user_id`, `created_at`, `likes_count`.

For each user, rank their tweets by likes (highest first). Show each user's top 3 most-liked tweets. Include tweet_id, user_id, likes_count, and rank.

**Expected Output**: user_id, tweet_id, likes_count, tweet_rank

---

### Problem 2: Instagram - Top Post Per Day

**Difficulty**: Easy | **Real-world Use Case**: Daily Trending Content

You have a `posts` table with columns: `post_id`, `created_date`, `likes_count`.

For each day, find the single most-liked post. Include post_id, created_date, likes_count, and rank within that day.

**Expected Output**: created_date, post_id, likes_count, daily_rank

---

### Problem 3: Spotify - User's Most Played Songs Ranking

**Difficulty**: Easy | **Real-world Use Case**: User Music Taste

You have a `listening_history` table with columns: `user_id`, `song_id`, `song_name`, `play_count`.

For each user, rank their songs by play count (most played first). Show each user's songs with their rank. Include user_id, song_name, play_count, and rank.

**Expected Output**: user_id, song_name, play_count, rank

---

### Problem 4: YouTube - Top 5 Channels by Subscribers Per Country

**Difficulty**: Easy-Medium | **Real-world Use Case**: Regional Content Strategy

You have a `channels` table with columns: `channel_id`, `channel_name`, `country`, `subscriber_count`.

For each country, rank channels by subscriber count. Show the top 5 channels per country. Include channel_name, country, subscriber_count, and rank.

**Expected Output**: country, channel_id, channel_name, subscriber_count, country_rank

---

### Problem 5: Amazon - Customer's Top 5 Purchases By Spend

**Difficulty**: Easy-Medium | **Real-world Use Case**: Customer Segmentation

You have an `orders` table with columns: `order_id`, `customer_id`, `order_amount`, `order_date`.

For each customer, rank orders by amount spent (highest first). Show each customer's top 5 orders. Include customer_id, order_id, order_amount, and rank.

**Expected Output**: customer_id, order_id, order_amount, order_rank

---

### Problem 6: Netflix - Latest Watched Content Per User Per Genre

**Difficulty**: Easy-Medium | **Real-world Use Case**: User Recommendations

You have tables:
- `watch_history` (user_id, content_id, watch_date, genre)
- `content` (content_id, title, genre)

For each user and genre combination, show only the most recently watched item. Include user_id, genre, content title, and watch_date. Order by user_id, then watch_date descending.

**Expected Output**: user_id, genre, content_title, watch_date

---

### Problem 7: Uber - Driver's Highest-Rated Rides

**Difficulty**: Easy-Medium | **Real-world Use Case**: Driver Quality Metrics

You have a `rides` table with columns: `ride_id`, `driver_id`, `rating`, `ride_date`.

For each driver, show their top 3 highest-rated rides. Include driver_id, ride_id, rating, and rank.

**Expected Output**: driver_id, ride_id, rating, ride_rank

---

### Problem 8: LinkedIn - Top Skill Per User

**Difficulty**: Easy-Medium | **Real-world Use Case**: User Profile Strengths

You have a `user_skills` table with columns: `user_id`, `skill_name`, `endorsement_count`.

For each user, show their most-endorsed skill (the one with highest endorsements). Include user_id, skill_name, and endorsement_count.

**Expected Output**: user_id, skill_name, endorsement_count

---

### Problem 9: Google Drive - Largest File In Each Folder

**Difficulty**: Easy-Medium | **Real-world Use Case**: Storage Optimization

You have a `files` table with columns: `file_id`, `folder_id`, `file_name`, `file_size_mb`.

For each folder, find the single largest file. Include folder_id, file_name, and file_size_mb.

**Expected Output**: folder_id, file_id, file_name, file_size_mb

---

### Problem 10: WhatsApp - Most Active Conversation Per User

**Difficulty**: Easy-Medium | **Real-world Use Case**: User Engagement

You have a `messages` table with columns: `user_id`, `conversation_id`, `message_count`, `last_message_date`.

For each user, find the conversation where they sent the most messages. Include user_id, conversation_id, and message_count.

**Expected Output**: user_id, conversation_id, message_count

