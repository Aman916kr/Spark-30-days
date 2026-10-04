````
# Day 19 — Broadcast Join

## Objective
Learn how to join a large fact DataFrame with a small reference DataFrame using Broadcast Join and compare it with Shuffle Sort Merge Join.

## Concepts Covered
- Broadcast Join
- `broadcast()` function
- Fact DataFrame and reference DataFrame
- Broadcast Hash Join
- Shuffle Sort Merge Join
- Execution plan using `explain(true)`
- Revenue aggregation by region

## Project Structure
```text
spark-day19/
├── build.sbt
├── data/
│   ├── transactions.csv
│   └── branch_master.csv
└── src/
    └── main/
        └── scala/
            └── BroadcastJoinAnalysis.scala
````

## Dataset

transactions.csv contains transaction details such as transaction ID, branch ID, customer ID, date, and amount.

branch_master.csv contains branch details such as branch ID, branch name, city, and region.

The transactions dataset represents the large fact table, while the branch master represents the small reference table.

## Implementation

### 1. Read DataFrames

Read both CSV files using Spark DataFrame APIs.

### 2. Broadcast Join

Use `broadcast()` to mark the small branch DataFrame for broadcasting.

```
val broadcastResult = transactions.join(
  broadcast(branches),
  col("t.branch_id") === col("b.branch_id"),
  "inner"
)
```

### 3. Revenue by Region

Group transactions by region and calculate transaction count and total revenue.

```
broadcastResult.groupBy(col("b.region"))
  .agg(
    count("*").alias("total_transactions"),
    sum(col("t.amount")).alias("total_revenue")
  )
  .orderBy(col("b.region"))
  .show(false)
```

### 4. Compare Join Execution Plans

Use `explain(true)` to inspect the execution plan.

```
broadcastResult.explain(true)
```

The application also performs a regular join without explicitly using `broadcast()`.

## Expected Output

### Broadcast Join Result

```
+--------------+---------+------------------+---------+------+-----------+------+
|transaction_id|branch_id|branch_name       |city     |region|customer_id|amount|
+--------------+---------+------------------+---------+------+-----------+------+
|T001          |B001     |Hyderabad Main    |Hyderabad|South |C101       |2500  |
|T002          |B002     |Bengaluru Central |Bengaluru|South |C102       |1800  |
|T003          |B003     |Mumbai Fort       |Mumbai   |West  |C103       |3200  |
|T004          |B001     |Hyderabad Main    |Hyderabad|South |C104       |1500  |
|T005          |B004     |Delhi North       |Delhi    |North |C105       |4200  |
|T006          |B005     |Chennai Central   |Chennai  |South |C106       |900   |
|T007          |B006     |Pune Main         |Pune     |West  |C107       |2700  |
|T008          |B007     |Kolkata East      |Kolkata  |East  |C108       |3500  |
|T009          |B008     |Ahmedabad West    |Ahmedabad|West  |C109       |1200  |
|T010          |B002     |Bengaluru Central |Bengaluru|South |C110       |2800  |
|T011          |B003     |Mumbai Fort       |Mumbai   |West  |C111       |4500  |
|T012          |B004     |Delhi North       |Delhi    |North |C112       |1600  |
|T013          |B005     |Chennai Central   |Chennai  |South |C113       |3300  |
|T014          |B006     |Pune Main         |Pune     |West  |C114       |2100  |
|T015          |B001     |Hyderabad Main    |Hyderabad|South |C115       |5000  |
+--------------+---------+------------------+---------+------+-----------+------+
```

### Revenue by Region

```
+------+------------------+-------------+
|region|total_transactions|total_revenue|
+------+------------------+-------------+
|East  |1                 |3500         |
|North |2                 |5800         |
|South |7                 |14500        |
|West  |5                 |11800        |
+------+------------------+-------------+
```

### Execution Plan

Look for a broadcast-related operator, commonly:

```
BroadcastHashJoin
```

The regular join may also use broadcast automatically because the sample branch dataset is small.

## How to Run

```
cd ~/spark-30-days/spark-day19
sbt run
```
