# Pattern 2: Filtering with Subqueries - Easy Solutions

## Solution Walkthrough

This section provides SQL solutions with detailed explanations. The key is understanding when to use IN vs NOT IN vs EXISTS vs NOT EXISTS.

---

## Problem 1 Solution: Twitter - Users Who Have Never Posted

**SQL Query (Using NOT IN):**
```sql
SELECT 
    user_id,
    user_name,
    created_at
FROM users
WHERE user_id NOT IN (
    SELECT DISTINCT user_id 
    FROM tweets
)
ORDER BY created_at;
```

**SQL Query (Using NOT EXISTS - More Efficient):**
```sql
SELECT 
    u.user_id,
    u.user_name,
    u.created_at
FROM users u
WHERE NOT EXISTS (
    SELECT 1 
    FROM tweets t 
    WHERE t.user_id = u.user_id
)
ORDER BY u.created_at;
```

**Explanation:**
- **NOT IN approach**: Finds user_ids NOT in the list of users who posted
- **NOT EXISTS approach**: Checks if no tweet exists for each user (more efficient for large datasets)
- The inner query finds all users who DID post
- NOT reverses the logic to find who DIDN'T post
- `SELECT 1` in NOT EXISTS is a convention (the value doesn't matter, only existence)
- The correlation `WHERE t.user_id = u.user_id` links outer and inner query

**Key Learning:**
- NOT EXISTS is generally faster than NOT IN (better performance)
- EXISTS/NOT EXISTS requires a correlated subquery (references outer table)
- Use SELECT 1 in EXISTS/NOT EXISTS (the column value doesn't matter)

**When to Use Each:**
- Use NOT IN: Simple cases, small datasets
- Use NOT EXISTS: Large datasets, need better performance

---

## Problem 2 Solution: Instagram - Posts Without Any Comments

**SQL Query:**
```sql
SELECT 
    p.post_id,
    p.user_id,
    p.created_at
FROM posts p
WHERE p.post_id NOT IN (
    SELECT DISTINCT post_id 
    FROM comments
)
ORDER BY p.created_at DESC;
```

**Alternative with NOT EXISTS:**
```sql
SELECT 
    p.post_id,
    p.user_id,
    p.created_at
FROM posts p
WHERE NOT EXISTS (
    SELECT 1 
    FROM comments c 
    WHERE c.post_id = p.post_id
)
ORDER BY p.created_at DESC;
```

**Explanation:**
- This finds posts where no comment rows exist
- Subquery identifies all post_ids that HAVE comments
- NOT IN reverses it to find posts WITHOUT comments
- This is a common problem: finding "zero engagement" items

**Key Learning:**
- This pattern (posts without comments) is very common
- It works for: orders without items, products without sales, users without activity, etc.
- NOT EXISTS is typically more efficient here

---

## Problem 3 Solution: Spotify - Artists With Zero Monthly Listeners

**SQL Query:**
```sql
SELECT 
    a.artist_id,
    a.artist_name
FROM artists a
WHERE NOT EXISTS (
    SELECT 1 
    FROM monthly_listeners ml 
    WHERE ml.artist_id = a.artist_id
)
ORDER BY a.artist_id;
```

**Explanation:**
- Finds artists where no monthly_listeners record exists
- This identifies artists who may need onboarding or are newly created
- Real-world: Quality check - should artists have listener data?
- NOT EXISTS is very efficient here

**Key Learning:**
- NOT EXISTS is the best pattern for "does a related record exist?"
- This pattern appears in: quality checks, data validation, onboarding flows

---

## Problem 4 Solution: YouTube - Channels With No Videos

**SQL Query:**
```sql
SELECT 
    c.channel_id,
    c.channel_name
FROM channels c
WHERE NOT EXISTS (
    SELECT 1 
    FROM videos v 
    WHERE v.channel_id = c.channel_id
)
ORDER BY c.created_at;
```

**Explanation:**
- Finds channels where no video record exists
- This is critical for YouTube: validating channel health
- Channels without videos might be abandoned or test accounts
- Real query: identify channels needing content

**Key Learning:**
- This pattern (parent entity without child records) is very common in real databases
- Works for: orders without items, carts without products, users without activities, etc.

---

## Problem 5 Solution: Amazon - Products Never Ordered

**SQL Query:**
```sql
SELECT 
    p.product_id,
    p.product_name
FROM products p
WHERE p.product_id NOT IN (
    SELECT DISTINCT product_id 
    FROM order_items
)
ORDER BY p.created_at;
```

**Alternative with NOT EXISTS:**
```sql
SELECT 
    p.product_id,
    p.product_name
FROM products p
WHERE NOT EXISTS (
    SELECT 1 
    FROM order_items oi 
    WHERE oi.product_id = p.product_id
)
ORDER BY p.created_at;
```

**Explanation:**
- Finds products that have never been ordered
- Real-world: Catalog cleanup, inventory management
- Questions: Why aren't these products selling? Should we remove them?
- This is a key business question for marketplace operations

**Key Learning:**
- This pattern finds "items with zero activity"
- Critical for: inventory management, catalog optimization, quality control

---

## Problem 6 Solution: Netflix - Users Who Watched Nothing

**SQL Query:**
```sql
SELECT 
    u.user_id,
    u.signup_date
FROM users u
WHERE u.user_id NOT IN (
    SELECT DISTINCT user_id 
    FROM watch_history
)
ORDER BY u.signup_date;
```

**Alternative with NOT EXISTS:**
```sql
SELECT 
    u.user_id,
    u.signup_date
FROM users u
WHERE NOT EXISTS (
    SELECT 1 
    FROM watch_history wh 
    WHERE wh.user_id = u.user_id
)
ORDER BY u.signup_date;
```

**Explanation:**
- Finds users with zero engagement (never watched content)
- These are critical for retention analysis
- Real question: Which users signed up but never engaged?
- Answer helps with: onboarding improvements, re-engagement campaigns

**Key Learning:**
- This pattern (zero activity) is essential for engagement analytics
- Helps identify: churn risk, onboarding failures, acquisition quality

---

## Problem 7 Solution: Uber - Drivers With Zero Rides

**SQL Query:**
```sql
SELECT 
    d.driver_id,
    d.driver_name
FROM drivers d
WHERE NOT EXISTS (
    SELECT 1 
    FROM rides r 
    WHERE r.driver_id = d.driver_id
)
ORDER BY d.signup_date;
```

**Explanation:**
- Finds drivers who signed up but never completed a ride
- Real-world use: Driver onboarding analysis
- Questions: Why didn't these drivers take any rides? Did they abandon?
- Can be used to: identify driver quality issues, improve onboarding

**Key Learning:**
- This pattern appears in any marketplace with supply (drivers, sellers, creators)
- Helps identify: platform onboarding effectiveness, supply quality

---

## Problem 8 Solution: LinkedIn - Users With No Connections

**SQL Query:**
```sql
SELECT 
    u.user_id,
    u.user_name
FROM users u
WHERE NOT EXISTS (
    SELECT 1 
    FROM connections c 
    WHERE c.user_id_1 = u.user_id 
       OR c.user_id_2 = u.user_id
)
ORDER BY u.created_at;
```

**Explanation:**
- Finds users who haven't connected with anyone
- Must check BOTH user_id_1 and user_id_2 (connection can go either way)
- Uses OR because a user might be user_id_1 in one row and user_id_2 in another
- Real question: What % of users form their first connection within 30 days?

**Key Learning:**
- For bidirectional relationships (friendships, connections), check both columns
- Use OR to combine multiple conditions
- This pattern: finding isolated entities, network analysis, onboarding insights

---

## Problem 9 Solution: Google Drive - Files By Inactive Users

**SQL Query:**
```sql
SELECT 
    f.file_id,
    f.file_name,
    u.last_login_date
FROM files f
INNER JOIN users u ON f.user_id = u.user_id
WHERE u.user_id IN (
    SELECT user_id 
    FROM users 
    WHERE DATEDIFF(CURDATE(), last_login_date) > 90
)
ORDER BY f.created_at;
```

**Alternative using NOT EXISTS pattern:**
```sql
SELECT 
    f.file_id,
    f.file_name,
    u.last_login_date
FROM files f
INNER JOIN users u ON f.user_id = u.user_id
WHERE NOT EXISTS (
    SELECT 1 
    FROM users u2 
    WHERE u2.user_id = u.user_id 
    AND DATEDIFF(CURDATE(), u2.last_login_date) <= 90
)
ORDER BY f.created_at;
```

**Explanation:**
- Uses IN to find users inactive for 90+ days
- Then finds their files
- Real-world: Data cleanup, orphaned file detection
- Questions: Should we alert users about their inactive accounts?
- Real-world use: GDPR compliance, data retention policies

**Key Learning:**
- IN with date filtering is common
- DATEDIFF calculates days between dates
- This pattern: temporal filtering, compliance, data cleanup

---

## Problem 10 Solution: WhatsApp - Empty Conversations

**SQL Query:**
```sql
SELECT 
    c.conversation_id,
    c.created_at
FROM conversations c
WHERE NOT EXISTS (
    SELECT 1 
    FROM messages m 
    WHERE m.conversation_id = c.conversation_id
)
ORDER BY c.created_at;
```

**Explanation:**
- Finds conversations with zero messages
- These might be: created but abandoned, test conversations, etc.
- Real-world: Conversation quality checks, cleanup of test data
- Similar to "empty carts" in e-commerce

**Key Learning:**
- NOT EXISTS for checking if child records exist
- This pattern: finding incomplete/abandoned entities
- Common in: messaging apps, e-commerce (empty carts), SaaS (empty projects)

---

## Pattern 2 Summary: Key Takeaways

### When to Use Subqueries:
✅ Finding items NOT in a list (NOT IN)
✅ Finding items WITHOUT related records (NOT EXISTS)
✅ Finding items WITH related records (EXISTS)
✅ Filtering by computed values (scalar subqueries)
✅ Breaking down complex logic into steps

### Subquery Types:
| Type | Use Case | Example |
|------|----------|---------|
| IN | Value exists in list | `WHERE user_id IN (SELECT...)` |
| NOT IN | Value doesn't exist in list | `WHERE user_id NOT IN (SELECT...)` |
| EXISTS | Related record exists | `WHERE EXISTS (SELECT 1 FROM...)` |
| NOT EXISTS | No related record exists | `WHERE NOT EXISTS (SELECT 1 FROM...)` |
| Scalar | Returns single value | `WHERE price > (SELECT AVG(price)...)` |

### IN vs EXISTS Performance:
- **IN**: Better for small result sets
- **EXISTS**: Better for large datasets, more efficient
- **NOT EXISTS**: Generally the most efficient for existence checks

### Common Patterns You'll See:
1. **Zero Activity Pattern**: "Users who never posted", "Products never ordered"
2. **Quality Check Pattern**: "Channels with no videos", "Artists with no listeners"
3. **Retention Pattern**: "Users inactive for 90 days", "Customers who stopped purchasing"
4. **Isolation Pattern**: "Users with no connections", "Files by inactive users"

### Things to Remember:
✓ Use DISTINCT in IN subqueries (optional but recommended)
✓ Use SELECT 1 in EXISTS/NOT EXISTS (the value doesn't matter)
✓ Correlate with parentheses: `WHERE t.user_id = u.user_id`
✓ Test with small data first (subqueries can be slow!)
✓ Consider JOIN as alternative (often more performant)

### Common Mistakes:
❌ Using IN with a very large result set (slow!)
❌ Using NOT IN when NULL values exist in result (returns no rows!)
❌ Forgetting DISTINCT in IN subquery (not wrong, just inefficient)
❌ Not correlating properly in EXISTS queries
❌ Using multiple levels of nesting (harder to read, sometimes slower)

