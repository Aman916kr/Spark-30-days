# Day10Partitioning

A beginner-friendly Apache Spark project that demonstrates RDD partitioning using `getNumPartitions`, `repartition`, `coalesce`, and `partitionBy`.

The project also demonstrates how increasing the number of partitions can help optimize a dataset suffering from too few partitions.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Spark Partitioning Concepts Covered

* RDD Partitions
* `getNumPartitions`
* `repartition`
* `coalesce`
* `partitionBy`
* `HashPartitioner`
* Pair RDD partitioning
* Parallelism
* Partition optimization

## Project Structure

```text id="74x4id"
Day10Partitioning/
├── build.sbt
├── README.md
└── src/
    └── main/
        └── scala/
            └── PartitioningDemo.scala
```

## Practice Tasks

### 1. Inspect Partition Counts

Create an RDD with two partitions:

```scala id="y0g1bf"
val numbers = sc.parallelize(1 to 20, 2)
```

Check the number of partitions:

```scala id="a2e7qx"
numbers.getNumPartitions
```

Output:

```text id="b55r4j"
2
```

The project also uses `mapPartitionsWithIndex` to inspect the data inside each partition.

### 2. repartition

Increase the number of partitions from 2 to 4.

```scala id="g2w0ka"
val repartitioned = numbers.repartition(4)
```

The operation performs a shuffle to redistribute the data.

```text id="2t1t9f"
2 partitions
      ↓
   Shuffle
      ↓
4 partitions
```

### 3. coalesce

Reduce the number of partitions from 4 to 2.

```scala id="3v1d5m"
val coalesced = repartitioned.coalesce(2)
```

`coalesce` is commonly used when the number of partitions needs to be reduced.

```text id="0z0h5s"
4 partitions
      ↓
  coalesce(2)
      ↓
2 partitions
```

### 4. When to Increase Partitions

Increasing partitions can help when a dataset has too few partitions and available CPU resources are not being fully utilized.

Example:

```text id="n1v4ji"
2 partitions
      ↓
4 partitions
```

More partitions can provide more parallelism, provided there are enough resources and the additional task overhead is worthwhile.

### 5. When to Decrease Partitions

Decreasing partitions can be useful when a dataset has become smaller after filtering or aggregation and too many small partitions would create unnecessary task overhead.

Example:

```text id="3ly5yq"
4 partitions
      ↓
2 partitions
```

### 6. Pair RDD

Create a Pair RDD containing account IDs and transaction amounts.

```scala id="9oxvbr"
val transactions = sc.parallelize(
  Seq(
    ("A101", 5000),
    ("A102", 3000),
    ("A101", 2000),
    ("A103", 7000)
  ),
  2
)
```

The Pair RDD structure is:

```text id="0o0qjs"
(Account ID, Amount)
```

### 7. partitionBy

Use `partitionBy` with a `HashPartitioner` to partition the Pair RDD by key.

```scala id="8s4sv6"
val partitionedTransactions =
  transactions.partitionBy(new HashPartitioner(4))
```

The resulting RDD has:

```text id="f9y7r9"
4 partitions
```

Records with the same key are assigned according to the partitioner's key-based partitioning.

### 8. Account Aggregation

After partitioning the Pair RDD, calculate total transactions by account.

```scala id="d5v8jf"
val accountTotals =
  partitionedTransactions.reduceByKey(_ + _)
```

Example:

```text id="y0a2pu"
A101 → 5000
A101 → 2000
```

becomes:

```text id="1umxj4"
A101 → 7000
```

## Optimization Scenario

### Problem

The dataset starts with only two partitions:

```text id="g8s7ob"
Dataset
   ↓
2 partitions
```

The dataset has too few partitions for the available parallelism.

### Solution

Use `repartition`:

```scala id="n7y6ks"
val repartitioned = numbers.repartition(4)
```

The result is:

```text id="8c0o9k"
2 partitions
      ↓
repartition(4)
      ↓
4 partitions
```

This provides more partitions that can be processed in parallel.

### Important Note

Increasing the number of partitions does not automatically make a job faster.

`repartition` introduces a shuffle, which has a cost. The number of partitions should be chosen based on dataset size, available resources, workload, and task overhead.

## How to Run

Run the following commands from the project directory.

### Compile

```bash id="5o7t1f"
sbt compile
```

### Run

```bash id="r5jj8w"
sbt run
```

## Output

The program displays the original partition count, partition contents, repartitioned data, coalesced partition count, Pair RDD partitioning, account totals, and the optimization scenario.

Example output:

```text id="m4f9bd"
========== ORIGINAL PARTITIONS ==========

Number of partitions: 2

========== ORIGINAL PARTITION DATA ==========

Partition 0 -> 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
Partition 1 -> 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

========== REPARTITION ==========

Partitions after repartition: 4

========== REPARTITIONED DATA ==========

Partition 0 -> ...
Partition 1 -> ...
Partition 2 -> ...
Partition 3 -> ...

========== COALESCE ==========

Partitions after coalesce: 2

========== PAIR RDD ==========

(A101,5000)
(A102,3000)
(A101,2000)
(A103,7000)
(A102,4000)
(A104,6000)
(A101,1000)
(A103,3000)

========== PARTITIONBY ==========

Partitions after partitionBy: 4

========== PARTITIONED TRANSACTIONS ==========

Partition 0 -> ...
Partition 1 -> ...
Partition 2 -> ...
Partition 3 -> ...

========== ACCOUNT TOTALS ==========

(A101,8000)
(A102,7000)
(A103,10000)
(A104,6000)

========== OPTIMIZATION SCENARIO ==========

Initial partitions: 2
Dataset has too few partitions.
Increasing partitions to 4 using repartition().
More partitions allow more parallel processing.

Optimization:
2 partitions -> 4 partitions

========== COALESCE SCENARIO ==========

Coalesce reduces the number of partitions.
It is useful when the dataset has too many partitions.
Example: 4 partitions -> 2 partitions
```

> **Note:** The exact records assigned to each partition can vary depending on Spark's partitioning behavior. The partition counts and account totals are the important deterministic results.

## Partitioning Flow

```text id="q8o3u6"
Dataset
   ↓
RDD
   ↓
Inspect Partitions
   ↓
2 Partitions
   ↓
repartition(4)
   ↓
4 Partitions
   ↓
coalesce(2)
   ↓
2 Partitions
```

For the Pair RDD:

```text id="wqf5hm"
Transactions
      ↓
Pair RDD
      ↓
partitionBy(HashPartitioner(4))
      ↓
4 Key-Based Partitions
      ↓
reduceByKey()
      ↓
Account Totals
```

## Key Learning

* A partition is a logical chunk of an RDD.
* `getNumPartitions` returns the number of partitions.
* `repartition` can increase or decrease partitions and performs a shuffle.
* `coalesce` is commonly used to decrease the number of partitions with less data movement than a full repartition.
* `partitionBy` partitions Pair RDDs according to their keys.
* More partitions can provide more parallelism when resources are available.
* Too many partitions can create unnecessary task overhead.
* Increasing partitions does not automatically improve performance.
* Partitioning should be selected based on data size and workload.

## Learning Outcome

After completing Day 10, I can:

* Inspect RDD partition counts.
* Inspect data across partitions.
* Use `repartition`.
* Use `coalesce`.
* Explain when to increase partitions.
* Explain when to decrease partitions.
* Use `partitionBy` on a Pair RDD.
* Explain key-based partitioning.
* Understand the relationship between partitions and parallelism.
* Optimize a dataset suffering from too few partitions.
