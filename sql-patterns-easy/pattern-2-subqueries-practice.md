# Pattern 2: Filtering with Subqueries - Easy Practice Problems

## Understanding the Pattern

Subqueries (also called nested queries or inner queries) let you use the result of one query inside another query. This pattern is perfect for filtering with complex logic: "find users who did X but not Y" or "find items above average."

**Key Concepts You'll Learn**:
- What is a subquery?
- IN vs NOT IN for checking if value is in a list
- EXISTS vs NOT EXISTS for checking if records exist
- Scalar subqueries (returns one value)
- Using subqueries in WHERE clause

**Why This Matters**: This tests logical thinking more than memorization. Real problems require filtering data in creative ways.

---

## Practice Problems

### Problem 1: Twitter - Users Who Have Never Posted

**Difficulty**: Easy | **Real-world Use Case**: Inactive User Detection

You have tables:
- `users` (user_id, user_name, created_at)
- `tweets` (tweet_id, user_id, created_at)

Find all users who exist in the users table but have never posted a tweet. Include user_id and user_name. Order by created_at (oldest accounts first).

**Expected Output**: user_id, user_name, created_at

---

### Problem 2: Instagram - Posts Without Any Comments

**Difficulty**: Easy | **Real-world Use Case**: Low Engagement Detection

You have tables:
- `posts` (post_id, user_id, created_at)
- `comments` (comment_id, post_id, user_id, created_at)

Find all posts that have zero comments. Include post_id, user_id, and created_at date. Order by created_at descending.

**Expected Output**: post_id, user_id, created_at

---

### Problem 3: Spotify - Artists With Zero Monthly Listeners

**Difficulty**: Easy | **Real-world Use Case**: Artist Onboarding/Quality Check

You have tables:
- `artists` (artist_id, artist_name, created_at)
- `monthly_listeners` (listener_id, artist_id, month, listener_count)

Find artists who have never had any monthly listener data recorded. Include artist_id and artist_name.

**Expected Output**: artist_id, artist_name

---

### Problem 4: YouTube - Channels With No Videos

**Difficulty**: Easy | **Real-world Use Case**: Channel Quality Validation

You have tables:
- `channels` (channel_id, channel_name, created_at)
- `videos` (video_id, channel_id, title, published_at)

Find all channels that have been created but have never published any video. Include channel_id and channel_name. Order by created_at.

**Expected Output**: channel_id, channel_name

---

### Problem 5: Amazon - Products Never Ordered

**Difficulty**: Easy-Medium | **Real-world Use Case**: Catalog Cleanup

You have tables:
- `products` (product_id, product_name, price, created_at)
- `order_items` (order_id, product_id, quantity)

Find all products that exist but have never been ordered. Include product_id and product_name. Order by created_at.

**Expected Output**: product_id, product_name

---

### Problem 6: Netflix - Users Who Watched Nothing (Zero Activity)

**Difficulty**: Easy-Medium | **Real-world Use Case**: Inactive User Segmentation

You have tables:
- `users` (user_id, email, signup_date)
- `watch_history` (watch_id, user_id, content_id, watch_date)

Find all users who signed up but never watched anything. Include user_id and signup_date. Order by signup_date (show oldest inactive users first).

**Expected Output**: user_id, signup_date

---

### Problem 7: Uber - Drivers With Zero Rides Completed

**Difficulty**: Easy-Medium | **Real-world Use Case**: Driver Onboarding Status

You have tables:
- `drivers` (driver_id, driver_name, signup_date)
- `rides` (ride_id, driver_id, completed_at)

Find drivers who signed up but have not completed any rides. Include driver_id and driver_name. Order by signup_date.

**Expected Output**: driver_id, driver_name

---

### Problem 8: LinkedIn - Users With No Connections

**Difficulty**: Easy-Medium | **Real-world Use Case**: Network Quality Check

You have tables:
- `users` (user_id, user_name, created_at)
- `connections` (connection_id, user_id_1, user_id_2, connected_at)

Find users who have never made any connections. Include user_id and user_name. A connection can be initiated by either user, so check both user_id_1 and user_id_2 columns.

**Expected Output**: user_id, user_name

---

### Problem 9: Google Drive - Files Owned By Inactive Users

**Difficulty**: Easy-Medium | **Real-world Use Case**: Orphaned Files Detection

You have tables:
- `users` (user_id, user_name, last_login_date)
- `files` (file_id, user_id, created_at, file_name)

Find files owned by users who haven't logged in for more than 90 days. Include file_id, file_name, and the user's last_login_date. Assume today's date for calculations.

**Expected Output**: file_id, file_name, last_login_date

---

### Problem 10: WhatsApp - Conversations With No Messages

**Difficulty**: Easy-Medium | **Real-world Use Case**: Conversation Cleanup

You have tables:
- `conversations` (conversation_id, created_at, conversation_type)
- `messages` (message_id, conversation_id, sent_at, message_text)

Find all conversations where no messages were ever sent. Include conversation_id and created_at. Order by created_at.

**Expected Output**: conversation_id, created_at

