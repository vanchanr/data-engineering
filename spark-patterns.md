### The Five Core Spark Patterns (Covers ~80% of Data Engineering Interview Questions)

Research shows that approximately 80% of FAANG data engineering interviews on Apache Spark focus on five repeatable patterns. Mastering these is more valuable than attempting random Spark topics.

#### Pattern 1: DataFrame Operations & Transformations

**Why FAANG tests this**: Understanding efficient data manipulation is fundamental to data engineering. Spark DataFrames are the standard abstraction for working with distributed data, and optimizing transformations is critical for performance.

**What you need to know**:
- DataFrame creation from various sources (CSV, Parquet, JSON, SQL databases)
- Lazy evaluation and action vs transformation distinction
- Basic transformations: select, filter, map, flatMap, withColumn
- Handling schema inference and explicit schema definition
- Column operations and expressions
- Handling missing data and data types

**Practice focus areas**:
- Choosing between RDD, DataFrame, and SQL API
- Writing efficient column expressions vs UDFs
- Casting and type conversions
- Handling nulls properly (fillna, dropna, coalesce)
- Optimizing data loading for large datasets

**Code snippet - DataFrame Creation & Basic Transformations**:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("data_eng").getOrCreate()

# Create DataFrame from data
data = [
    ("alice", 25, "New York"),
    ("bob", 30, "San Francisco"),
    ("charlie", None, "Austin")
]
df = spark.createDataFrame(data, ["name", "age", "city"])

# Basic transformations
df_transformed = (df
    .select(col("name"), col("age"), col("city"))  # Select columns
    .filter(col("age") > 25)  # Filter rows
    .withColumn("name_upper", upper(col("name")))  # Add column
    .withColumn("full_info", concat(col("name"), lit("-"), col("city")))  # Concatenate
)

df_transformed.show()
```

**Code snippet - Handling Missing Data & Type Conversions**:
```python
from pyspark.sql.functions import coalesce, when, fillna, isnull

# Handle null values
df_cleaned = (df
    .fillna({"age": 0})  # Fill age nulls with 0
    .fillna({"city": "Unknown"})  # Fill city nulls
)

# Conditional logic
df_with_category = df_cleaned.withColumn(
    "age_group",
    when(col("age") < 18, "minor")
    .when(col("age") < 65, "adult")
    .otherwise("senior")
)

# Coalesce: return first non-null value
df_backup = df.withColumn("location", coalesce(col("city"), lit("Remote")))

# Check for nulls
df_null_check = df.select([
    col(c).alias(c + "_is_null") 
    for c in df.columns
]).select([when(isnull(c), 1).otherwise(0).alias(c) for c in df.columns])
```

**Real-world scenarios**: Data ingestion pipelines, ETL preprocessing, data cleansing, feature engineering

---

#### Pattern 2: Aggregations & GroupBy Operations

**Why FAANG tests this**: Most analytics questions require computing metrics and KPIs. Understanding group by, aggregations, and their performance characteristics is essential for building scalable analytics pipelines.

**What you need to know**:
- GROUP BY operations and multi-level aggregations
- Aggregate functions (count, sum, avg, min, max, stddev, approx_quantile)
- Custom aggregations with UserDefinedAggregateFunction (UDAF)
- HAVING clauses for filtering aggregated results
- Handling skewed data in group by operations
- Optimization: bucketing vs repartitioning

**Practice focus areas**:
- Multi-level grouping (group by multiple columns)
- Computing multiple aggregates in single pass
- Approximate aggregations for large datasets
- Detecting and handling data skew
- Null handling in group by operations

**Code snippet - Basic Aggregations & GroupBy**:
```python
from pyspark.sql.functions import (
    count, sum, avg, min, max, stddev, 
    approx_quantile, collect_list, expr
)

# Basic aggregations
stats_df = df.groupBy("city").agg(
    count("*").alias("total_users"),
    avg("age").alias("avg_age"),
    min("age").alias("min_age"),
    max("age").alias("max_age"),
    stddev("age").alias("stddev_age")
).filter(col("total_users") > 1)

stats_df.show()

# Multiple aggregations in single pass
multi_agg = df.groupBy("city").agg(
    count("*").alias("count"),
    sum("age").alias("total_age"),
    avg("age").alias("avg_age"),
    min("age").alias("min_age")
)

# Approximate quantiles for large datasets
quantile_df = df.groupBy("city").agg(
    approx_quantile("age", [0.25, 0.5, 0.75]).alias("quantiles")
)
```

**Code snippet - Handling Skewed Data in GroupBy**:
```python
# Detect skew
from pyspark.sql.functions import rand, monotonically_increasing_id

# Add salt to skewed keys to distribute work
def handle_skew(df, skew_col, salt_range=10):
    """Add salt to skewed grouping column"""
    return df.withColumn(
        f"{skew_col}_salted",
        concat(col(skew_col), lit("-"), (rand() * salt_range).cast("int"))
    )

df_salted = handle_skew(df, "city", salt_range=5)

# Aggregate with salted key, then aggregate again
intermediate_agg = df_salted.groupBy("city_salted").agg(
    sum("age").alias("total_age")
)

final_agg = intermediate_agg.withColumn(
    "city", expr("split(city_salted, '-')[0]")
).groupBy("city").agg(
    sum("total_age").alias("total_age")
)

# Alternative: use repartition before groupBy for better distribution
balanced_agg = (df
    .repartition(100, "city")
    .groupBy("city")
    .agg(count("*").alias("count"))
)
```

**Real-world scenarios**: KPI calculation, revenue reporting, user analytics, daily/monthly aggregations, anomaly detection

---

#### Pattern 3: Joins & Data Shuffling

**Why FAANG tests this**: Joins are ubiquitous in data engineering. Optimizing them requires understanding broadcast vs shuffle joins, handling data skew, and knowing when to repartition.

**What you need to know**:
- Join types: inner, left/right outer, full outer, cross, anti, semi
- Broadcast joins vs shuffle joins and when to use each
- Join optimization: column selection, filtering before joins
- Handling data skew in large joins
- Detecting and avoiding cartesian products
- Memory considerations for broadcast variables

**Practice focus areas**:
- Choosing appropriate join type for problem
- Predicting join performance characteristics
- Optimizing join order for multiple joins
- Handling skewed join keys
- Using broadcast for small dimension tables
- Partitioning strategy for large joins

**Code snippet - Basic Joins & Broadcast Join**:
```python
from pyspark.sql.functions import broadcast
from pyspark.sql import functions as F

# Create sample DataFrames
users_df = spark.createDataFrame([
    (1, "alice", "NYC"),
    (2, "bob", "SF"),
    (3, "charlie", "LA")
], ["user_id", "name", "city"])

orders_df = spark.createDataFrame([
    (101, 1, 100.50),
    (102, 1, 250.00),
    (103, 2, 75.00),
    (104, 3, 500.00)
], ["order_id", "user_id", "amount"])

# Regular shuffle join
joined_df = (orders_df
    .join(users_df, on="user_id", how="inner")
    .select("order_id", "name", "city", "amount")
)

# Broadcast join (for small dimension tables)
small_users = users_df.filter(col("city").isin("NYC", "SF"))
broadcast_joined = (orders_df
    .join(broadcast(small_users), on="user_id", how="left")
    .select("order_id", "name", "city", "amount")
)

# Join with multiple conditions
multi_condition_join = (orders_df
    .join(users_df, 
          (orders_df.user_id == users_df.user_id) & 
          (orders_df.amount > 100),
          how="inner")
)
```

**Code snippet - Handling Skewed Joins**:
```python
# Detect skew before join
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

def detect_skew(df, key_col, percentile=0.99):
    """Detect skewed keys in join column"""
    window_spec = Window.orderBy(col(key_col))
    skew_stats = df.groupBy(key_col).count().agg(
        max("count").alias("max_count"),
        avg("count").alias("avg_count")
    ).collect()[0]
    
    skew_ratio = skew_stats.max_count / skew_stats.avg_count
    print(f"Skew ratio: {skew_ratio}")
    return skew_ratio > 10  # Threshold for skew

# Strategy 1: Split skewed key
def split_skewed_join(left_df, right_df, join_key, salt_partitions=10):
    """Handle skewed key by splitting into multiple partitions"""
    
    # Identify skewed keys
    skew_keys = (left_df.groupBy(join_key).count()
                 .filter(col("count") > 1000)
                 .select(join_key)
                 .collect())
    skew_keys_list = [row[join_key] for row in skew_keys]
    
    # Split skewed data
    left_skewed = left_df.filter(col(join_key).isin(skew_keys_list))
    left_normal = left_df.filter(~col(join_key).isin(skew_keys_list))
    
    right_skewed = right_df.filter(col(join_key).isin(skew_keys_list))
    right_normal = right_df.filter(~col(join_key).isin(skew_keys_list))
    
    # Join normal data normally
    normal_joined = left_normal.join(right_normal, join_key, "inner")
    
    # Join skewed data with salting
    left_salted = left_skewed.withColumn(
        f"{join_key}_salt",
        concat(col(join_key), lit("-"), (rand() * salt_partitions).cast("int"))
    )
    right_replicated = right_skewed.withColumn(
        f"{join_key}_salt",
        expr(f"explode(array({', '.join([str(i) for i in range(salt_partitions)])}))")
    ).withColumn(
        f"{join_key}_salt",
        concat(col(join_key), lit("-"), col(f"{join_key}_salt").cast("int"))
    )
    
    skewed_joined = (left_salted
        .join(right_replicated, f"{join_key}_salt", "inner")
        .drop(f"{join_key}_salt")
    )
    
    return normal_joined.union(skewed_joined)
```

**Real-world scenarios**: Fact-dimension joins, data enrichment, merging multiple data sources, deduplication

---

#### Pattern 4: Window Functions & Advanced Analytics

**Why FAANG tests this**: Window functions enable sophisticated analytics like ranking, running totals, and trend analysis. They're essential for calculating KPIs and behavioral metrics.

**What you need to know**:
- Window functions: row_number, rank, dense_rank, lag, lead
- Over clause with partition by and order by
- Frame specifications (unbounded preceding, current row, etc.)
- First/last value and nth value operations
- Cumulative operations (sum, avg, count)
- Performance implications of window operations

**Practice focus areas**:
- Top N per group queries
- Detecting change points (lag/lead)
- Running aggregations and cumulative metrics
- Ranking with and without ties
- Time-series operations
- Handling gaps in time series

**Code snippet - Window Functions Basics**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank, lag, lead, 
    first_value, last_value, sum as spark_sum
)

# Sample data with time series
data = [
    ("alice", "2024-01-01", 100),
    ("alice", "2024-01-02", 150),
    ("alice", "2024-01-03", 120),
    ("bob", "2024-01-01", 200),
    ("bob", "2024-01-02", 180),
]
sales_df = spark.createDataFrame(data, ["user", "date", "amount"])

# Define window specification
window_spec = Window.partitionBy("user").orderBy("date")

# Ranking functions
ranking_df = sales_df.withColumn(
    "row_num", row_number().over(window_spec)
).withColumn(
    "rank", rank().over(window_spec)
).withColumn(
    "dense_rank", dense_rank().over(window_spec)
)

# Lead/lag for detecting changes
change_df = sales_df.withColumn(
    "prev_amount", lag("amount", 1).over(window_spec)
).withColumn(
    "next_amount", lead("amount", 1).over(window_spec)
).withColumn(
    "amount_change", col("amount") - col("prev_amount")
)

ranking_df.show()
change_df.show()
```

**Code snippet - Cumulative Aggregations & Top N**:
```python
# Top 2 products by sales per region
top_products_spec = Window.partitionBy("region").orderBy(col("sales").desc())

top_df = (sales_data
    .withColumn("rank", rank().over(top_products_spec))
    .filter(col("rank") <= 2)
    .drop("rank")
)

# Cumulative sum over time
cumsum_spec = (Window
    .partitionBy("user")
    .orderBy("date")
    .rangeBetween(Window.unboundedPreceding, 0)
)

cumsum_df = sales_df.withColumn(
    "cumulative_sales", spark_sum("amount").over(cumsum_spec)
)

# 7-day moving average
moving_window = (Window
    .partitionBy("user")
    .orderBy(col("date"))
    .rangeBetween(-6, 0)  # 7 days including current
)

moving_avg_df = sales_df.withColumn(
    "moving_avg_7d", avg("amount").over(moving_window)
)

# First and last values within partition
first_last_spec = Window.partitionBy("user")

first_last_df = sales_df.withColumn(
    "first_date", first_value("date").over(first_last_spec)
).withColumn(
    "last_date", last_value("date").over(first_last_spec.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
)

cumsum_df.show()
```

**Real-world scenarios**: Ranking products/users, cohort analysis, retention curves, trend detection, anomaly detection

---

#### Pattern 5: Performance Optimization & Tuning

**Why FAANG tests this**: Understanding Spark's execution model, optimization strategies, and operational concerns separates junior from senior engineers. This determines scalability and cost-effectiveness.

**What you need to know**:
- Partitioning strategies and repartitioning
- Caching/persistence and memory management
- Broadcast variables for efficient small data distribution
- SQL query plans and the Catalyst optimizer
- Shuffle operations and their costs
- Resource allocation (executors, cores, memory)
- Spill and out-of-memory handling

**Practice focus areas**:
- Choosing optimal partition count
- Predicting shuffle behavior
- Identifying performance bottlenecks
- Tuning for specific workloads
- Cost-performance trade-offs
- Monitoring and metrics
- Skew and stragglers

**Code snippet - Partitioning & Caching Strategy**:
```python
from pyspark.sql.functions import spark_partition_id
from pyspark.storagelevel import StorageLevel

# Check current partitions
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# Optimal partition count = number of cores * 2-3
optimal_partitions = spark.sparkContext.defaultParallelism * 2
df_repartitioned = df.repartition(optimal_partitions)

# Repartition by key for optimized joins/groupby
df_partitioned_by_key = df.repartition(200, "user_id")

# View partition distribution
partition_dist = (df
    .withColumn("partition_id", spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

# Bucketing for joining on same key repeatedly
df.write.bucketBy(100, "user_id").mode("overwrite").saveAsTable("users_bucketed")

# Caching strategies
df.persist(StorageLevel.MEMORY_ONLY)  # Cache in memory
df.persist(StorageLevel.MEMORY_AND_DISK)  # Spill to disk if needed
df.persist(StorageLevel.DISK_ONLY)  # Only disk

# Use cache before expensive operations
df_frequent_use = df.filter(col("age") > 25)
df_frequent_use.cache()

result1 = df_frequent_use.count()
result2 = df_frequent_use.groupBy("city").count().collect()

df_frequent_use.unpersist()  # Release memory
```

**Code snippet - Query Plan Analysis & Broadcast Variables**:
```python
# Explain query plan
df.explain(mode="extended")  # Shows physical and logical plans
df.explain(mode="cost")      # Shows cost estimation

# Filter before expensive operations
# Bad: large join then filter
bad_query = large_left.join(large_right, "key").filter(col("amount") > 1000)

# Good: filter then join
good_query = (large_left
    .filter(col("amount") > 1000)
    .join(large_right.filter(col("status") == "active"), "key")
)

# Broadcast small table
small_lookup = spark.createDataFrame([
    ("US", "United States"),
    ("CA", "Canada")
], ["code", "country"])

broadcast_var = broadcast(small_lookup)

enriched_df = (transactions_df
    .join(broadcast_var, 
          transactions_df.country_code == broadcast_var.code,
          "left")
)

# Using broadcast variables in UDFs
states_broadcast = spark.broadcast({
    "NY": "New York",
    "CA": "California"
})

def get_state_name(code):
    return states_broadcast.value.get(code, "Unknown")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

get_state_udf = udf(get_state_name, StringType())
result_df = df.withColumn("state_name", get_state_udf(col("state_code")))
```

**Code snippet - Handling Skew & Stragglers**:
```python
# Monitor task duration to identify stragglers
from pyspark.sql.functions import hash, abs

# Adaptive query execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Manual skew handling: add random salt
def handle_shuffle_skew(df, key_col, num_salt_buckets=10):
    """Distribute skewed keys across multiple partitions"""
    from pyspark.sql.functions import rand, monotonically_increasing_id
    
    # Find heavy hitters
    heavy_hitters = (df.groupBy(key_col).count()
                    .filter(col("count") > 1000)
                    .select(key_col))
    
    # Split heavy hitters with salt
    is_heavy = df.join(
        heavy_hitters, 
        key_col, 
        "left_semi"
    )
    
    is_light = df.join(
        heavy_hitters,
        key_col,
        "left_anti"
    )
    
    salted = is_heavy.withColumn(
        f"{key_col}_salted",
        concat(col(key_col), lit("_"), (rand() * num_salt_buckets).cast("int"))
    )
    
    return salted.union(is_light)

# Limit broadcast variable size
broadcast_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"Auto broadcast threshold: {broadcast_threshold}")

# Explicitly tell Spark not to broadcast
result = (df1
    .join(df2, "key", "inner")
    .hint("shuffle_hash")  # Use hash join instead of broadcast
)
```

**Real-world scenarios**: ETL optimization, large-scale data processing, ML feature engineering, real-time analytics pipelines

---

### Quick Reference: Interview Preparation Checklist

- [ ] **DataFrames**: Understand lazy evaluation, transformations vs actions, schema handling
- [ ] **Aggregations**: Multi-level grouping, handling skew, approximate aggregations
- [ ] **Joins**: Know broadcast vs shuffle, identify skew, optimize join order
- [ ] **Window Functions**: Ranking, lag/lead, cumulative operations, frame specifications
- [ ] **Performance**: Partitioning strategy, caching, query plan analysis, bottleneck identification
- [ ] **Optimization**: Consider filter-before-join, use broadcast for small tables, avoid unnecessary shuffles
- [ ] **Monitoring**: Monitor executor memory, shuffle read/write, task duration for stragglers
- [ ] **Scalability**: Design for billion-row datasets, think about cost and execution time trade-offs

### Key Spark Configuration for Interviews

```python
spark = (SparkSession.builder
    .appName("data_pipeline")
    .config("spark.sql.adaptive.enabled", "true")  # Adaptive query execution
    .config("spark.sql.adaptive.skewJoin.enabled", "true")  # Skew handling
    .config("spark.default.parallelism", "200")  # Default partitions
    .config("spark.sql.shuffle.partitions", "200")  # Shuffle partitions
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB broadcast threshold
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "4")
    .config("spark.driver.memory", "2g")
    .getOrCreate())
```
