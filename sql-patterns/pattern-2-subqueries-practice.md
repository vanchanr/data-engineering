# Pattern 2: Filtering with Subqueries - Practice Problems

## Understanding the Pattern

Filtering with subqueries tests your logical thinking and ability to handle complex filtering scenarios. These patterns are essential for recommendation systems, anomaly detection, and customer segmentation.

**Key Concepts**:
- Subquery vs JOIN vs CTE trade-offs
- EXISTS vs IN vs NOT EXISTS performance considerations
- Correlated subqueries (reference outer query)
- Scalar subqueries (return single value)
- Multi-level nested filtering

---

## Practice Problems

### Problem 1: Twitter - Users Who Never Posted (Engagement Gap Analysis)
**Difficulty**: Medium | **Real-world Use Case**: User Activation & Engagement

You have tables:
- `users` (user_id, user_name, created_at)
- `tweets` (tweet_id, user_id, created_at)
- `followers` (user_id, follower_id)

Find all users who have followers but have never posted a tweet. Include their user name and follower count. Order by follower count descending.

**Expected Output Columns**: user_id, user_name, follower_count

---

### Problem 2: Instagram - Posts Without Comments Below Average Engagement
**Difficulty**: Medium | **Real-world Use Case**: Content Quality Analysis

You have tables:
- `posts` (post_id, user_id, created_at, likes)
- `comments` (comment_id, post_id, user_id, created_at)

Find posts that have zero comments AND have fewer likes than the average likes per post. Include post_id, user_id, likes count, and average likes. Order by likes ascending.

**Expected Output Columns**: post_id, user_id, likes, avg_likes_all_posts

---

### Problem 3: Amazon - Products Never Purchased
**Difficulty**: Medium | **Real-world Use Case**: Inventory & Product Catalog Management

You have tables:
- `products` (product_id, product_name, category_id, created_date)
- `order_items` (order_item_id, product_id, order_id)

Find all products that exist in the catalog but have never been purchased. Show product name, category, and days since creation. Order by creation date (oldest first).

**Expected Output Columns**: product_id, product_name, category_id, days_since_creation

---

### Problem 4: Netflix - Users Watching Content Outside Their Subscription Tier
**Difficulty**: Medium-High | **Real-world Use Case**: Compliance & Content Access Control

You have tables:
- `users` (user_id, subscription_tier) [tiers: basic, standard, premium]
- `watch_history` (watch_id, user_id, content_id)
- `content` (content_id, required_tier, title)

Find users with "basic" subscription who have watched content requiring "premium" subscription. Include user_id and list of content titles they shouldn't have access to.

**Expected Output Columns**: user_id, unauthorized_content_titles

---

### Problem 5: Spotify - Artists Whose Top Track is Below Their Average
**Difficulty**: Medium-High | **Real-world Use Case**: Artist Performance Anomaly Detection

You have tables:
- `artists` (artist_id, artist_name)
- `tracks` (track_id, artist_id, track_name, streams)

Find artists where their most popular track (by streams) has fewer streams than their artist's average streams per track. This identifies inconsistent popularity. Include artist name and the gap between their top track and average.

**Expected Output Columns**: artist_id, artist_name, top_track_name, top_track_streams, avg_streams_per_track, gap

---

### Problem 6: YouTube - Viewers Who Watched Same Creator's Videos on Different Days
**Difficulty**: Medium-High | **Real-world Use Case**: Viewer Loyalty & Habit Tracking

You have tables:
- `channels` (channel_id, channel_name, creator_id)
- `videos` (video_id, channel_id, published_date)
- `watch_history` (watch_id, user_id, video_id, watched_date)

Find viewers who have watched videos from the same creator on at least 5 different days within a 30-day period. Include viewer_id, creator name, count of days watched, and date range.

**Expected Output Columns**: user_id, creator_name, days_watched_on, date_range_start, date_range_end

---

### Problem 7: Uber - Drivers Who Accept More Rides Than Average
**Difficulty**: Medium-High | **Real-world Use Case**: Driver Performance Incentives

You have tables:
- `drivers` (driver_id, driver_name, signup_date, vehicle_type)
- `ride_requests` (request_id, driver_id, request_date, accepted)
- `rides` (ride_id, request_id, ride_date, fare_amount)

Find drivers in the last 90 days who have:
1. Accepted ride requests at a rate above average
2. Completed rides with average fare above the platform average
3. Have been drivers for more than 6 months

Include driver info, acceptance rate, and avg fare.

**Expected Output Columns**: driver_id, driver_name, vehicle_type, months_as_driver, acceptance_rate, avg_fare

---

### Problem 8: Google Docs - Documents Modified by Users Who Never Created Them
**Difficulty**: High | **Real-world Use Case**: Collaboration Analytics & Access Control

You have tables:
- `documents` (doc_id, creator_id, created_date, title)
- `document_edits` (edit_id, doc_id, editor_id, edit_date, changes_count)

Find documents that have been edited by users other than the creator, AND where the non-creator edits exceed the creator's own edits. Include document info and the users who edited more than the creator.

**Expected Output Columns**: doc_id, title, creator_id, editor_id, creator_edit_count, non_creator_edit_count

---

### Problem 9: LinkedIn - Jobs Not Applied By Users From Target Audience
**Difficulty**: High | **Real-world Use Case**: Job Recommendation & Opportunity Gap Analysis

You have tables:
- `users` (user_id, user_name, current_title, experience_years)
- `job_postings` (job_id, job_title, required_experience_years)
- `applications` (application_id, user_id, job_id)

Find job postings where no user with matching experience level has applied, but at least 10 such matching users exist in the system. Include job info and count of eligible users who didn't apply.

**Expected Output Columns**: job_id, job_title, required_experience_years, eligible_user_count, users_who_did_not_apply

---

### Problem 10: WhatsApp - Users in Group Chats Who Never Sent Messages
**Difficulty**: High | **Real-world Use Case**: Group Engagement & Moderation Analytics

You have tables:
- `group_members` (group_id, user_id, added_date)
- `groups` (group_id, group_name, created_date, member_count)
- `messages` (message_id, group_id, sender_id, sent_timestamp, message_text)

Find users who are members of groups that have high message activity (1000+ messages), but the user themselves have never sent any message in those groups. Include user_id, group_name, group's total message count, and user's membership duration.

**Expected Output Columns**: user_id, group_id, group_name, total_group_messages, membership_duration_days, messages_sent_by_user
