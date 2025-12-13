# FAANG SQL Patterns - Quick Reference & Cheat Sheet

## Pattern 1: Aggregation with GROUP BY

### Quick Syntax
```sql
SELECT 
    grouping_column,
    COUNT(*) as row_count,
    SUM(amount) as total,
    AVG(value) as average,
    MIN(metric) as minimum,
    MAX(metric) as maximum
FROM table_name
WHERE condition_on_raw_data    -- Filters BEFORE aggregation
GROUP BY grouping_column
HAVING aggregate_condition      -- Filters AFTER aggregation
ORDER BY aggregate_column DESC;
```

### Key Points
- **WHERE** filters rows before GROUP BY
- **HAVING** filters groups after GROUP BY (must use aggregate function)
- All non-aggregated columns must be in GROUP BY
- NULL values are treated as a distinct group
- Empty groups are excluded

### Common Mistakes
❌ `WHERE COUNT(*) > 10` - Wrong! Use HAVING instead
❌ `SELECT column` without GROUP BY - Wrong! Need it in GROUP BY

### Real Interview Question
"Find daily revenue for the last 30 days, only days with 100+ transactions"

---

## Pattern 2: Filtering with Subqueries

### Quick Syntax - EXISTS
```sql
SELECT *
FROM table1 t1
WHERE EXISTS (
    SELECT 1 FROM table2 t2 
    WHERE t2.id = t1.id AND t2.condition
)
```

### Quick Syntax - IN
```sql
SELECT *
FROM table1
WHERE id IN (
    SELECT id FROM table2 WHERE condition
)
```

### Quick Syntax - NOT EXISTS
```sql
SELECT *
FROM table1
WHERE NOT EXISTS (
    SELECT 1 FROM table2 
    WHERE table2.id = table1.id
)
```

### Key Points
- **EXISTS**: Stops on first match (efficient)
- **IN**: Requires full result set
- **NOT IN with NULL**: Returns NO rows! Use NOT EXISTS instead
- **Correlated subquery**: References outer query (runs per row)
- **Non-correlated**: Runs once, result set reused

### Performance Hierarchy (Best to Worst)
1. EXISTS / NOT EXISTS (recommended)
2. JOIN / LEFT JOIN
3. IN (with small lists)
4. NOT IN (use NOT EXISTS instead)

### Common Mistakes
❌ `WHERE id NOT IN (SELECT id FROM table WHERE condition = NULL)`
✅ `WHERE NOT EXISTS (SELECT 1 FROM table WHERE table.id = outer.id AND condition)`

### Real Interview Question
"Find users with followers who have never posted a tweet"

---

## Pattern 3: Ranking with Window Functions

### Quick Syntax - Basic Ranking
```sql
SELECT 
    column1,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rn,
    RANK() OVER (PARTITION BY category ORDER BY value DESC) as rank_val,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY value DESC) as dense_rank
FROM table_name
WHERE condition
ORDER BY category, rn;
```

### Quick Syntax - LAG/LEAD
```sql
SELECT 
    date,
    revenue,
    LAG(revenue) OVER (PARTITION BY user_id ORDER BY date) as prev_day_revenue,
    LEAD(revenue) OVER (PARTITION BY user_id ORDER BY date) as next_day_revenue,
    revenue - LAG(revenue) OVER (PARTITION BY user_id ORDER BY date) as daily_change
FROM sales
ORDER BY user_id, date;
```

### Quick Syntax - NTILE
```sql
SELECT 
    product_id,
    price,
    NTILE(4) OVER (PARTITION BY category ORDER BY price) as price_quartile
FROM products;
-- Q1 = 0-25%, Q2 = 25-50%, Q3 = 50-75%, Q4 = 75-100%
```

### Ranking Comparison Table
| Function | With Ties | Result |
|----------|-----------|--------|
| ROW_NUMBER() | Unique | 1, 2, 3, 4 |
| RANK() | Same rank, skip | 1, 1, 3, 4 |
| DENSE_RANK() | Same rank, no skip | 1, 1, 2, 3 |

### Key Points
- **PARTITION BY**: Resets ranking for each group
- **ORDER BY**: Determines rank order
- **Frame**: `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` = 7-row window
- **QUALIFY**: Filter window functions (cleaner than CTE)

### Common Mistakes
❌ `ROW_NUMBER() ... ORDER BY random_column` - Gives inconsistent results
✅ `RANK() ... ORDER BY metric DESC` - Clear ordering

### Real Interview Question
"Find top 3 products by revenue for each category"

---

## Pattern 4: Moving Averages & Cumulative Sums

### Quick Syntax - 7-Day Moving Average
```sql
SELECT 
    date,
    daily_value,
    AVG(daily_value) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7day
FROM daily_metrics
ORDER BY date;
```

### Quick Syntax - Cumulative Sum
```sql
SELECT 
    date,
    daily_amount,
    SUM(daily_amount) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_total
FROM transactions
ORDER BY date;
```

### Quick Syntax - Percent of Total
```sql
SELECT 
    date,
    revenue,
    SUM(revenue) OVER (
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue,
    SUM(revenue) OVER (
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as total_revenue,
    ROUND(
        SUM(revenue) OVER (
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) / SUM(revenue) OVER (
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) * 100, 2
    ) as percent_of_total
FROM sales
ORDER BY date;
```

### Frame Clause Meanings
- `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` = 7-row window
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` = Cumulative from start
- `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` = All rows
- `ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING` = Current + next row

### Key Points
- **ROWS**: Physical row count (best for most cases)
- **RANGE**: Date-based ranges (be careful with duplicates)
- First N rows have incomplete windows (N-1 previous rows)
- NULL handling: NULLs are ignored in SUM/AVG

### Common Mistakes
❌ `LAG(value) OVER (ORDER BY date) as prev_value` then using prev_value in WHERE
✅ Use CTE to calculate LAG first, then filter in outer query

### Real Interview Question
"Calculate 30-day moving average of daily active users with month-over-month growth"

---

## Pattern 5: Conditional Aggregations (CASE WHEN)

### Quick Syntax - Multiple Metrics
```sql
SELECT 
    date,
    COUNT(*) as total_orders,
    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue,
    SUM(CASE WHEN status = 'cancelled' THEN amount ELSE 0 END) as cancelled_revenue,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_count,
    ROUND(
        COUNT(CASE WHEN status = 'cancelled' THEN 1 END) / COUNT(*) * 100, 2
    ) as cancellation_rate
FROM orders
GROUP BY date
ORDER BY date DESC;
```

### Quick Syntax - Segmentation
```sql
SELECT 
    user_id,
    CASE 
        WHEN lifetime_value > 5000 THEN 'High'
        WHEN lifetime_value > 1000 THEN 'Medium'
        ELSE 'Low'
    END as value_segment,
    CASE 
        WHEN purchase_frequency >= 10 THEN 'Frequent'
        WHEN purchase_frequency >= 3 THEN 'Regular'
        ELSE 'Infrequent'
    END as frequency_segment,
    COUNT(*) as segment_count
FROM customer_metrics
GROUP BY value_segment, frequency_segment;
```

### Quick Syntax - Rate Calculation
```sql
SELECT 
    category,
    SUM(CASE WHEN metric = 'success' THEN count ELSE 0 END) as success_count,
    SUM(CASE WHEN metric IN ('success', 'failure') THEN count ELSE 0 END) as total_count,
    ROUND(
        SUM(CASE WHEN metric = 'success' THEN count ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN metric IN ('success', 'failure') THEN count ELSE 0 END), 0) * 100, 2
    ) as success_rate
FROM event_metrics
GROUP BY category;
```

### Key Points
- **CASE WHEN inside aggregate**: `SUM(CASE WHEN ... THEN value ELSE 0 END)`
- **ELSE 0** for SUM (don't use NULL - it's ignored)
- **Multiple CASE**: More efficient than multiple queries
- **NULLIF**: Prevent division by zero: `COUNT(*) / NULLIF(total, 0)`
- **DISTINCT with CASE**: `COUNT(DISTINCT CASE WHEN condition THEN id END)`

### Common Mistakes
❌ `SUM(CASE WHEN ... THEN value END)` - Returns NULL if no matches
✅ `SUM(CASE WHEN ... THEN value ELSE 0 END)` - Returns 0 if no matches

❌ `CASE status WHEN 'completed' THEN ...` without ELSE in aggregate
✅ `CASE WHEN status = 'completed' THEN value ELSE 0 END`

### Real Interview Question
"Calculate revenue breakdown by order status, with cancellation rates by category"

---

## Universal SQL Tips

### Query Optimization Checklist
- [ ] Use WHERE before GROUP BY (reduces rows early)
- [ ] Index on GROUP BY, JOIN, and WHERE columns
- [ ] Use EXISTS instead of IN
- [ ] Avoid SELECT * (specify columns)
- [ ] Use UNION instead of OR when possible
- [ ] Pre-filter with subquery if joining large tables
- [ ] Materialized views for complex calculations
- [ ] Test with EXPLAIN/ANALYZE

### NULL Handling
```sql
-- NULL is not equal to anything, even NULL
WHERE value = NULL      -- Returns nothing!
WHERE value IS NULL     -- Correct

-- In aggregates, NULL is ignored
SELECT COUNT(column)    -- Counts non-NULL only
SELECT COUNT(*)         -- Counts all rows
SELECT COUNT(DISTINCT column)  -- Distinct non-NULL values

-- NULLIF for division by zero
ROUND(sum_a / NULLIF(sum_b, 0), 2)
```

### Date Handling
```sql
-- Extract date from timestamp
DATE(timestamp_column)

-- Date arithmetic
DATE_ADD(date, INTERVAL 1 DAY)
DATE_SUB(date, INTERVAL 7 DAY)
DATEDIFF(current_date, date_column)

-- Grouping by period
DATE_TRUNC(date_column, MONTH)
EXTRACT(MONTH FROM date_column)
```

### Type Conversions
```sql
-- String to numeric
CAST(string_column AS INTEGER)
CAST(string_column AS DECIMAL(10,2))

-- Numeric to string
CAST(number_column AS VARCHAR)

-- Date formatting
DATE_FORMAT(date_column, '%Y-%m')
```

---

## Interview Day Checklist

### Before Writing SQL
- [ ] Understand the business question
- [ ] Identify data sources (tables)
- [ ] Sketch the logic on paper
- [ ] Clarify ambiguous requirements

### While Writing
- [ ] Start simple, then add complexity
- [ ] Use meaningful aliases
- [ ] Add comments for complex logic
- [ ] Test edge cases (NULL, empty sets, duplicates)

### Before Submitting
- [ ] Verify all columns in output match requirements
- [ ] Check for NULL values in results
- [ ] Consider performance (billions of rows)
- [ ] Discuss trade-offs with interviewer

### Sample Answer Structure
1. **Clarify** (30 seconds): "So I need to find X, grouped by Y, filtered by Z?"
2. **Approach** (30 seconds): "I'll use Pattern 2 with EXISTS to check for..."
3. **Code** (2-3 minutes): Write the SQL
4. **Test** (1 minute): Walk through with example
5. **Optimize** (1 minute): "With billions of rows, I'd add index on..."

---

## Pattern Selection Decision Tree

```
Question asks for...

├─ Summary/Totals/Averages by category?
│  └─ Pattern 1: Aggregation with GROUP BY
│
├─ Complex filtering (users who didn't do X)?
│  └─ Pattern 2: Subqueries (EXISTS/IN)
│
├─ Top N per group / Ranking / Nth highest?
│  └─ Pattern 3: Window Functions (ROW_NUMBER/RANK)
│
├─ Trends / Moving averages / Cumulative totals?
│  └─ Pattern 4: Window Functions (SUM OVER / AVG OVER)
│
└─ Multiple metrics / Revenue breakdown by type?
   └─ Pattern 5: Conditional Aggregation (CASE WHEN)
```

---

## Quick SQL Syntax Reference

### Common Aggregate Functions
```sql
COUNT(*)              -- Total rows
COUNT(column)         -- Non-NULL count
COUNT(DISTINCT col)   -- Unique values
SUM(column)           -- Total
AVG(column)           -- Average (excludes NULL)
MIN/MAX(column)       -- Minimum/Maximum
STRING_AGG(col, ',')  -- Concatenate (varies by DB)
```

### Common Date Functions
```sql
CURDATE() / TODAY()           -- Current date
DATEDIFF(date1, date2)        -- Days between
DATE_ADD/DATE_SUB             -- Add/subtract interval
DATE_TRUNC(date, MONTH)       -- Truncate to month
EXTRACT(MONTH FROM date)      -- Extract month number
```

### JOIN Types (By Pattern)
```sql
INNER JOIN   -- Rows in BOTH tables (most common)
LEFT JOIN    -- All from left + matching from right (Pattern 2)
RIGHT JOIN   -- All from right + matching from left
FULL JOIN    -- All rows from both
CROSS JOIN   -- Cartesian product (each row vs each row)
```

### Set Operations
```sql
UNION        -- Combine + remove duplicates
UNION ALL    -- Combine + keep duplicates (faster)
INTERSECT    -- Rows in BOTH queries
EXCEPT       -- Rows in first but not second
```

---

**Master these 5 patterns and you'll solve ~80% of FAANG SQL interview questions!** 🚀
