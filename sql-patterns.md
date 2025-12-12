### The Five Core SQL Patterns (Covers ~80% of Interview Questions)

Research shows that approximately 80% of FAANG SQL interview questions follow five repeatable patterns. Mastering these is more valuable than attempting random questions.

#### Pattern 1: Aggregation with GROUP BY

**Why FAANG tests this**: Teams operate on dashboards and metrics. This pattern mirrors daily work calculating KPIs, engagement metrics, retention rates, and revenue numbers.

**What you need to know**:
- Basic aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY syntax and execution order
- HAVING clause for filtering aggregated results
- Common real-world use cases: daily active users (DAU), monthly active users (MAU), revenue per customer, average session duration

**Practice focus areas**:
- Multi-level aggregation (aggregate by user, then by time period)
- Handling NULL values in aggregations
- Combining multiple aggregate functions in a single query
- Dealing with data quality issues (duplicates, missing values)

#### Pattern 2: Filtering with Subqueries (EXISTS, IN, NOT EXISTS)

**Why FAANG tests this**: This pattern tests logical thinking rather than memorization. Real data science problems require complex filtering logic.

**What you need to know**:
- Difference between IN vs EXISTS vs NOT EXISTS (EXISTS is more efficient for large datasets)
- Correlated vs non-correlated subqueries
- Scalar subqueries vs row-returning subqueries
- When to use subqueries vs CTEs vs joins

**Practice focus areas**:
- Find items a user hasn't interacted with
- Exclude users who already performed an action
- Find records above/below average
- Complex multi-step filtering logic

**Real-world scenarios**: Recommendation systems, anomaly detection, customer segmentation, funnel analysis

#### Pattern 3: Ranking with Window Functions

**Why FAANG tests this**: Window functions appear in almost every FAANG SQL interview. They're essential for analyzing behavioral trends, product rankings, and deep analytics.

**What you need to know**:
- ROW_NUMBER (assigns unique sequential integers, restarts for each partition)
- RANK (handles ties by assigning the same rank, then skipping)
- DENSE_RANK (handles ties by assigning the same rank, continuous numbering)
- NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE
- OVER clause with PARTITION BY and ORDER BY
- Frame specifications (ROWS BETWEEN, RANGE BETWEEN)

**Practice focus areas**:
- Top N items per category
- Nth highest/lowest values
- Cumulative calculations
- Relative rankings within groups

**Real-world scenarios**: Top products by revenue per region, most active users, trending topics, highest-performing channels

#### Pattern 4: Moving Averages & Cumulative Sums

**Why FAANG tests this**: Time-series analysis is critical for product analytics dashboards. Companies need to understand trends, growth patterns, and performance changes over time.

**What you need to know**:
- Calculating moving averages over time windows (7-day, 30-day rolling)
- Cumulative sum/count operations
- Handling date gaps and incomplete time series
- Window function frame clauses for time-based calculations
- Performance optimization for large time-series datasets

**Practice focus areas**:
- 7-day and 30-day rolling engagement metrics
- Month-over-month growth calculations
- Seasonal patterns and trends
- Retention curves and cohort analysis
- Handling missing days/gaps in data

**Real-world scenarios**: Revenue trends, user growth tracking, inventory fluctuations, website traffic patterns, customer churn rates

#### Pattern 5: Conditional Aggregations (CASE WHEN)

**Why FAANG tests this**: Enables computing multiple metrics in a single efficient query. Demonstrates understanding of data segmentation and categorical analysis.

**What you need to know**:
- CASE WHEN syntax for conditional logic
- Nested CASE statements
- CASE within aggregate functions (e.g., `SUM(CASE WHEN...)`)
- Multiple conditional branches in a single query
- NULL handling in conditional logic

**Practice focus areas**:
- Paid vs free user metrics
- New vs returning customers
- Completed vs cancelled vs failed transactions
- A/B testing control vs treatment groups
- Multi-stage funnel analysis
- Revenue attribution by source

**Real-world scenarios**: Subscription cohort analysis, experiment evaluation, revenue breakdowns, customer segmentation, compliance reporting
