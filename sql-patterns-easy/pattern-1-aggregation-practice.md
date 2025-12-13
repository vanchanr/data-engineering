# Pattern 1: Aggregation with GROUP BY - Easy Practice Problems

## Understanding the Pattern

Aggregation with GROUP BY is used to calculate metrics and roll-up data across different dimensions. This pattern helps you answer questions like "how much?", "how many?", and "what's the average?"

**Key Concepts You'll Learn**:
- Basic aggregates: COUNT, SUM, AVG, MIN, MAX
- Grouping data by a single dimension
- Filtering with WHERE (before grouping)
- Filtering with HAVING (after grouping)
- Multi-column GROUP BY

---

## Practice Problems

### Problem 1: Twitter - Total Tweets Per User
**Difficulty**: Easy | **Real-world Use Case**: User Activity Metrics

You're building a dashboard to show user engagement. You have a `tweets` table with columns: `tweet_id`, `user_id`, `created_at`, `likes_count`.

Write a query to count how many tweets each user has posted. Include the user_id and tweet count. Order by tweet count descending.

**Expected Output**: user_id, tweet_count

---

### Problem 2: Instagram - Total Followers Per User
**Difficulty**: Easy | **Real-world Use Case**: User Influence Ranking

You have a `followers` table with columns: `follower_id`, `user_id`, `followed_at`.

Write a query to count how many followers each user has. Include the user_id and follower count. Order by follower count descending and limit to top 10 users.

**Expected Output**: user_id, follower_count

---

### Problem 3: Spotify - Average Song Duration Per Artist
**Difficulty**: Easy | **Real-world Use Case**: Artist Profile Analytics

You have a `songs` table with columns: `song_id`, `artist_id`, `artist_name`, `duration_seconds`.

Write a query to calculate the average song duration for each artist. Include artist_id, artist_name, and average duration rounded to 2 decimal places. Show only artists with more than 5 songs.

**Expected Output**: artist_id, artist_name, avg_duration

---

### Problem 4: YouTube - Total Views Per Channel
**Difficulty**: Easy | **Real-world Use Case**: Channel Performance

You have a `videos` table with columns: `video_id`, `channel_id`, `channel_name`, `views`, `published_date`.

Write a query to find the total views for each channel. Include channel_id, channel_name, and total views. Order by total views descending.

**Expected Output**: channel_id, channel_name, total_views

---

### Problem 5: Amazon - Number of Orders Per Customer
**Difficulty**: Easy | **Real-world Use Case**: Customer Segmentation

You have an `orders` table with columns: `order_id`, `customer_id`, `order_date`, `order_amount`.

Write a query to count how many orders each customer has placed. Include customer_id and order count. Show only customers with at least 3 orders, ordered by order count descending.

**Expected Output**: customer_id, order_count

---

### Problem 6: Netflix - Average Watch Duration Per User
**Difficulty**: Easy-Medium | **Real-world Use Case**: User Engagement Metrics

You have a `watch_history` table with columns: `user_id`, `content_id`, `watch_date`, `duration_minutes`.

Write a query to calculate the average watch duration for each user. Include user_id and average duration rounded to 1 decimal place. Order by average duration descending and limit to top 20 users.

**Expected Output**: user_id, avg_watch_duration

---

### Problem 7: Uber - Total Revenue Per Driver
**Difficulty**: Easy-Medium | **Real-world Use Case**: Driver Earnings

You have a `rides` table with columns: `ride_id`, `driver_id`, `driver_name`, `ride_date`, `fare_amount`.

Write a query to calculate total revenue earned by each driver. Include driver_id, driver_name, and total revenue. Show only drivers with total revenue above $1000, ordered by total revenue descending.

**Expected Output**: driver_id, driver_name, total_revenue

---

### Problem 8: Google Drive - Files Per User Per Folder
**Difficulty**: Easy-Medium | **Real-world Use Case**: Storage Analytics

You have a `files` table with columns: `file_id`, `user_id`, `folder_id`, `created_at`, `file_size_mb`.

Write a query to count how many files each user has in each folder. Include user_id, folder_id, and file count. Order by user_id, then file count descending.

**Expected Output**: user_id, folder_id, file_count

---

### Problem 9: LinkedIn - Total Endorsements Per Skill
**Difficulty**: Easy-Medium | **Real-world Use Case**: Skill Popularity

You have an `endorsements` table with columns: `endorsement_id`, `user_id`, `skill_id`, `skill_name`, `endorsement_date`.

Write a query to count how many endorsements each skill has received. Include skill_id, skill_name, and endorsement count. Show only skills with at least 100 endorsements, ordered by endorsement count descending.

**Expected Output**: skill_id, skill_name, endorsement_count

---

### Problem 10: WhatsApp - Average Messages Per Conversation
**Difficulty**: Easy-Medium | **Real-world Use Case**: Engagement Analysis

You have a `messages` table with columns: `message_id`, `conversation_id`, `sender_id`, `sent_at`, `message_length`.

Write a query to calculate the average message length for each conversation. Include conversation_id and average message length rounded to 0 decimals. Order by average message length descending and show top 15 conversations.

**Expected Output**: conversation_id, avg_message_length

