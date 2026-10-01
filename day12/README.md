Day12CachePersist
A beginner-friendly Apache Spark project that demonstrates RDD caching and persistence by reusing a cleaned transaction dataset across multiple reports.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.9
- SBT 1.11.7
- Ubuntu Linux
- JDK 11

## Spark Cache and Persist Concepts Covered

- RDD caching
- `cache()`
- `persist()`
- `StorageLevel`
- `MEMORY_ONLY`
- `MEMORY_AND_DISK`
- `DISK_ONLY`
- `unpersist()`
- RDD reuse
- Performance optimization
- Memory management

## Project Structure

Day12CachePersist/
├── build.sbt
├── README.md
└── src/
    └── main/
        └── scala/
            └── Day12CachePersist.scala

## Practice Tasks

### 1. Create Transaction Dataset

Create an RDD containing transaction ID, account ID, amount, and transaction status.

### 2. Clean Transaction Dataset

Remove failed transactions and transactions with invalid amounts.

### 3. Cache the Cleaned Dataset

Use `cache()` to store the cleaned RDD because it will be reused by multiple reports.

### 4. Generate Total Transaction Amount

Calculate the total amount of all valid transactions.

Expected result:

Total Transaction Amount: 40500

### 5. Count Valid Transactions

Calculate the number of valid transactions.

Expected result:

Total Valid Transactions: 8

### 6. Calculate Account-wise Totals

Use `reduceByKey()` to calculate the total transaction amount for each account.

Expected result:

A101 -> 20000
A102 -> 3000
A103 -> 4000
A104 -> 5500
A105 -> 6000

### 7. Demonstrate Persist

Use `persist()` with a selected storage level.

### 8. Check Storage Level

Use `getStorageLevel` to inspect the storage level of an RDD.

### 9. Unpersist RDD

Release cached and persisted data using `unpersist()`.

## Cache vs Persist

### Cache

`cache()` uses Spark's default storage level and is useful when an RDD is reused by multiple actions.

### Persist

`persist()` allows a specific storage level to be selected and provides more control over how the RDD is stored.

## Storage Levels

Common storage levels:

- `MEMORY_ONLY`
- `MEMORY_AND_DISK`
- `DISK_ONLY`

## When Caching Helps

Caching is useful when:

- An RDD is reused by multiple actions
- Transformations are expensive
- The dataset can fit in memory
- The same dataset is required for multiple reports

## When Caching Hurts

Caching can hurt performance when:

- An RDD is used only once
- The dataset is very large
- Memory is insufficient
- Cached partitions are frequently evicted
- Recomputation is cheaper than storing the RDD

## Scenario

A company receives transaction data and first cleans the data by removing failed and invalid transactions.

The cleaned dataset is then reused for three reports:

- Total Transaction Amount
- Valid Transaction Count
- Account-wise Transaction Total

Caching allows Spark to reuse the cleaned RDD instead of repeatedly recomputing the same transformations.

## How to Run

Compile the project:

sbt compile

Run the application:

sbt run

## Output

The program displays:

- Cleaned transactions
- Total transaction amount
- Valid transaction count
- Account-wise transaction totals
- Cache information
- Persist storage level
- Persisted transaction count
- Cleanup information

Expected results:

Total Transaction Amount: 40500
Total Valid Transactions: 8

Account-wise totals:

A101 -> 20000
A102 -> 3000
A103 -> 4000
A104 -> 5500
A105 -> 6000

## Key Learning

- `cache()` provides simple RDD caching.
- `persist()` provides control over the storage level.
- Caching is useful when the same RDD is reused multiple times.
- Caching should not be used unnecessarily.
- `unpersist()` releases cached data when it is no longer required.
