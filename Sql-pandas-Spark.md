
# Side-by-side: **20 common data operations** in **SQL**, **pandas**, and **PySpark**

All examples assume a source table/dataframe named `orders` (columns shown when needed). `orders` columns used in examples: `order_id, user_id, product_id, amount, qty, order_ts (timestamp), category, tags (array/string array), city`

Notes: *SQL* is generic (ANSI + common DW features). *pandas* snippets assume `import pandas as pd` and a DataFrame `df`. *PySpark* snippets assume `from pyspark.sql import functions as F, Window` and a DataFrame `orders_df`.

## 0) Setup snippets (pandas / PySpark)

```python
# pandas
import pandas as pd
df = pd.read_parquet("orders.parquet")   # example load

# PySpark
from pyspark.sql import SparkSession, functions as F, Window
spark = SparkSession.builder.getOrCreate()
orders_df = spark.read.parquet("orders.parquet")
```

## 1) Select columns

**SQL**
```sql
SELECT order_id, user_id, amount FROM orders;
```

**pandas**
```python
df[['order_id', 'user_id', 'amount']]
```

**PySpark**
```python
orders_df.select('order_id', 'user_id', 'amount')
```

## 2) Filter rows (amount > 100)

**SQL**
```sql
SELECT * FROM orders WHERE amount > 100;
```

**pandas**
```python
df[df['amount'] > 100]
```

**PySpark**
```python
orders_df.filter(orders_df.amount > 100)  -- or orders_df.where(F.col('amount') > 100)
```

## 3) Add / derive column (total = amount * qty)

**SQL**
```sql
SELECT *, amount * qty AS total FROM orders;
```

**pandas**
```python
df['total'] = df['amount'] * df['qty']
```

**PySpark**
```python
orders_df = orders_df.withColumn('total', F.col('amount') * F.col('qty'))
```

## 4) Rename column

**SQL**
```sql
SELECT order_id AS id, user_id AS uid FROM orders;
```

**pandas**
```python
df.rename(columns={'order_id': 'id', 'user_id': 'uid'}, inplace=False)
```

**PySpark**
```python
orders_df.select(F.col('order_id').alias('id'), F.col('user_id').alias('uid'))
# or
orders_df.withColumnRenamed('order_id','id').withColumnRenamed('user_id','uid')
```

## 5) Drop column

**SQL**
```sql
-- not direct; select desired columns
SELECT order_id, user_id, amount FROM orders;
```

**pandas**
```python
df.drop(columns=['tags'], inplace=False)
```

**PySpark**
```python
orders_df.drop('tags')
```

## 6) Distinct values (distinct categories)

**SQL**
```sql
SELECT DISTINCT category FROM orders;
```

**pandas**
```python
df['category'].drop_duplicates()  # or df['category'].unique()
```

**PySpark**
```python
orders_df.select('category').distinct()
```

## 7) Group by aggregation (sum of amount per category)

**SQL**
```sql
SELECT category, SUM(amount) AS total_amount FROM orders GROUP BY category;
```

**pandas**
```python
df.groupby('category', as_index=False)['amount'].sum().rename(columns={'amount':'total_amount'})
```

**PySpark**
```python
orders_df.groupBy('category').agg(F.sum('amount').alias('total_amount'))
```

## 8) Multiple aggregations with aliases

**SQL**
```sql
SELECT category,
       SUM(amount) AS total_amount,
       AVG(amount) AS avg_amount,
       COUNT(*) AS orders_count
FROM orders
GROUP BY category;
```

**pandas**
```python
df.groupby('category').agg(
    total_amount = pd.NamedAgg(column='amount', aggfunc='sum'),
    avg_amount = pd.NamedAgg(column='amount', aggfunc='mean'),
    orders_count = pd.NamedAgg(column='order_id', aggfunc='count')
).reset_index()
```

**PySpark**
```python
orders_df.groupBy('category').agg(
    F.sum('amount').alias('total_amount'),
    F.avg('amount').alias('avg_amount'),
    F.count('*').alias('orders_count')
)
```

## 9) Inner join (orders × products table)

**SQL**
```sql
SELECT o.*, p.product_name
FROM orders o
JOIN products p
  ON o.product_id = p.product_id;
```

**pandas**
```python
products = pd.read_parquet('products.parquet')
df.merge(products, how='inner', left_on='product_id', right_on='product_id')
```

**PySpark**
```python
products_df = spark.read.parquet('products.parquet')
orders_df.join(products_df, on='product_id', how='inner')
```

## 10) Left join

**SQL**
```sql
SELECT o.*, p.product_name
FROM orders o
LEFT JOIN products p
  ON o.product_id = p.product_id;
```

**pandas**
```python
df.merge(products, how='left', on='product_id')
```

**PySpark**
```python
orders_df.join(products_df, on='product_id', how='left')
```

## 11) Union / Concatenate two tables (same schema)

**SQL**
```sql
SELECT * FROM orders_2024
UNION ALL
SELECT * FROM orders_2025;
```

**pandas**
```python
pd.concat([df_2024, df_2025], axis=0, ignore_index=True)
```

**PySpark**
```python
orders_2024_df.unionByName(orders_2025_df)   # unionByName preserves column order by name
# or orders_2024_df.union(orders_2025_df) if same schema/order
```

## 12) Order by + limit (Top 10 orders by amount)

**SQL**
```sql
SELECT * FROM orders ORDER BY amount DESC LIMIT 10;
```

**pandas**
```python
df.nlargest(10, 'amount')  # or df.sort_values('amount', ascending=False).head(10)
```

**PySpark**
```python
orders_df.orderBy(F.col('amount').desc()).limit(10)
```

## 13) Row_number per partition — Top N per group (Top 2 orders per user)

**SQL**
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn
  FROM orders
) t
WHERE rn <= 2;
```

**pandas**
```python
df['rn'] = df.groupby('user_id')['amount'].rank(method='first', ascending=False)
df[df['rn'] <= 2].drop(columns='rn')
```

**PySpark**
```python
w = Window.partitionBy('user_id').orderBy(F.col('amount').desc())
orders_df.withColumn('rn', F.row_number().over(w)).filter(F.col('rn') <= 2).drop('rn')
```

## 14) Lag — previous value per user (previous order amount)

**SQL**
```sql
SELECT *,
       LAG(amount) OVER (PARTITION BY user_id ORDER BY order_ts) AS prev_amount
FROM orders;
```

**pandas**
```python
df = df.sort_values(['user_id','order_ts'])
df['prev_amount'] = df.groupby('user_id')['amount'].shift(1)
```

**PySpark**
```python
w = Window.partitionBy('user_id').orderBy('order_ts')
orders_df.withColumn('prev_amount', F.lag('amount', 1).over(w))
```

## 15) Deduplicate — keep latest per (user_id, product_id) by order_ts

**SQL**
```sql
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_ts DESC) AS rn
  FROM orders
) t
WHERE rn = 1;
```

**pandas**
```python
df.sort_values('order_ts', ascending=False).drop_duplicates(subset=['user_id','product_id'], keep='first')
```

**PySpark**
```python
w = Window.partitionBy('user_id','product_id').orderBy(F.col('order_ts').desc())
orders_df.withColumn('rn', F.row_number().over(w)).filter(F.col('rn') == 1).drop('rn')
```

## 16) Rolling / moving average per user (7-day moving avg of amount)

*(Assume `order_ts` is datetime and data is daily or has a frequency)*

**SQL** *(if DB supports RANGE BETWEEN INTERVAL)*
```sql
SELECT user_id, order_ts,
       AVG(amount) OVER (
         PARTITION BY user_id
         ORDER BY order_ts
         RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW
       ) AS mov_avg_7d
FROM orders;
```

**pandas**
```python
df = df.sort_values(['user_id','order_ts'])
df['mov_avg_7d'] = df.groupby('user_id').apply(
    lambda g: g.set_index('order_ts')['amount'].rolling('7D').mean()
).reset_index(level=0, drop=True)
```

**PySpark** *(approx: use time-based window aggregation requires advanced patterns — easiest via flatMapGroupsWithState or use Spark 3.2+ SQL functions)* Simple example using window with rows (if daily data and no gaps):
```python
w = Window.partitionBy('user_id').orderBy('order_ts').rowsBetween(-6, 0)
orders_df.withColumn('mov_avg_7d', F.avg('amount').over(w))
```
*(For real date-range rolling window use Spark Structured Streaming or rangeBetween with timestamps in long form.)*

## 17) Cumulative sum per partition (running total per user)

**SQL**
```sql
SELECT *,
       SUM(amount) OVER (PARTITION BY user_id ORDER BY order_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM orders;
```

**pandas**
```python
df = df.sort_values(['user_id','order_ts'])
df['running_total'] = df.groupby('user_id')['amount'].cumsum()
```

**PySpark**
```python
w = Window.partitionBy('user_id').orderBy('order_ts').rowsBetween(Window.unboundedPreceding, 0)
orders_df.withColumn('running_total', F.sum('amount').over(w))
```

## 18) Pivot (category → columns, sum(amount))

**SQL**
```sql
SELECT city,
       SUM(CASE WHEN category = 'electronics' THEN amount ELSE 0 END) AS electronics,
       SUM(CASE WHEN category = 'clothing' THEN amount ELSE 0 END) AS clothing
FROM orders
GROUP BY city;
```
*(Or use PIVOT in DBs that support it)*

**pandas**
```python
df.pivot_table(index='city', columns='category', values='amount', aggfunc='sum', fill_value=0).reset_index()
```

**PySpark**
```python
orders_df.groupBy('city').pivot('category', ['electronics','clothing']).agg(F.sum('amount')).na.fill(0)
```

## 19) Explode array column (tags: array) → one row per tag

**SQL** *(BigQuery / Snowflake syntax shown conceptually)*
```sql
SELECT order_id, user_id, tag
FROM orders, UNNEST(tags) AS tag;
-- or CROSS JOIN UNNEST(tags) depending on dialect
```

**pandas** *(tags as list)*
```python
df.explode('tags')  # returns one row per tag
```

**PySpark**
```python
orders_df.select('order_id','user_id', F.explode('tags').alias('tag'))
```

## 20) Write dataframe to Parquet (persist results)

**SQL** *(in some systems use CTAS to external table; generic example for BigQuery/Snowflake omitted)*
```sql
-- In systems with external tables:
CREATE TABLE gold_orders AS SELECT * FROM orders_transformed;
-- Or use vendor-specific COPY/EXPORT commands to write Parquet.
```

**pandas**
```python
df.to_parquet('s3://my-bucket/gold/orders.parquet', index=False)  # or locally: df.to_parquet('orders.parquet', index=False)
```

**PySpark**
```python
orders_df.write.mode('overwrite').parquet('s3a://my-bucket/gold/orders.parquet')
# or for Delta:
orders_df.write.format('delta').mode('overwrite').save('/mnt/delta/gold/orders')
```

## Quick reference table (operation → where it shines)

| Operation | SQL | pandas | PySpark |
|---|---:|:---:|:---:|
| Select / Projection | ✅ fast in DW | ✅ interactive | ✅ distributed |
| Filtering | ✅ optimized | ✅ instant small data | ✅ distributed filter |
| Aggregations | ✅ OLAP strengths | ✅ small data | ✅ scales large |
| Window functions | ✅ strong (DW) | ✅ via groupby/shift/rolling | ✅ full-featured |
| Joins | ✅ optimized | ✅ small-to-medium | ✅ large-scale joins |
| Pivot | ✅ SQL/DB pivot | ✅ easy | ✅ supported via pivot |
| Explode arrays | ✅ supported in DW | ✅ explode | ✅ explode (scalable) |
| Rolling / time windows | ✅ good | ✅ great for EDA | ✅ production-grade |
| Writing Parquet | Vendor-specific | ✅ good for local | ✅ scalable write to S3/Delta |

## Interview tips (how to talk about differences)

- **When asked which to use in a design**, state: *If data fits in memory and you need quick analysis → pandas;* *If data is already in a DW and you need analytics/dashboards → SQL;* *If you need to process >10s of GBs, build ETL pipelines, or stream → Spark/PySpark.*

- **Performance note:** pandas is single-node; converting a pandas prototype to PySpark usually requires rethinking partitioning, avoiding groupBy on high-cardinality keys, and using broadcast joins for small dimension tables.

- **Correctness note:** SQL engines and Spark implement different null/ordering semantics — always be explicit about NULL handling and ORDER BY clauses in interview answers.
