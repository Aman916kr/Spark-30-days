# Day11BroadcastAccumulators

A beginner-friendly Apache Spark project that demonstrates Broadcast Variables and Accumulators using RDD processing.

The project uses a small product master table as broadcast data and validates transactions against it. An accumulator is used to count transactions containing invalid product IDs.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Spark Broadcast and Accumulator Concepts Covered

* Broadcast Variables
* `sc.broadcast`
* Accessing broadcast data using `.value`
* Accumulators
* `sc.longAccumulator`
* `add`
* Distributed RDD processing
* Read-only shared data
* Distributed counters
* Transaction validation
* Small master/reference data

## Practice Tasks

### 1. Create Product Master Data

Create a small product master table containing product IDs and product names.

```scala
val productMaster = Map(
  101 -> "Laptop",
  102 -> "Mouse",
  103 -> "Keyboard",
  104 -> "Monitor"
)
```

The product master contains:

```text
101 -> Laptop
102 -> Mouse
103 -> Keyboard
104 -> Monitor
```

This is small reference data that can be shared with Spark executors.

### 2. Broadcast Product Master

Broadcast the product master table:

```scala
val broadcastProducts = sc.broadcast(productMaster)
```

The broadcast variable distributes the small read-only dataset to the executors.

```text
                 Driver
                   |
            Product Master
                   |
               Broadcast
                   |
       +-----------+-----------+
       |           |           |
   Executor 1  Executor 2  Executor 3
```

The broadcast data can be accessed using:

```scala
broadcastProducts.value
```

### 3. Why Use Broadcast?

Broadcast variables are useful when a small dataset is required by many tasks.

Examples include:

```text
Product master
Country codes
Tax rates
Category mappings
Configuration data
```

Instead of repeatedly sending the same reference data with tasks, Spark can distribute it as a broadcast variable.

### 4. Create Transaction RDD

Create an RDD containing transaction ID, product ID, and transaction amount.

```scala
val transactions = sc.parallelize(
  Seq(
    (1, 101, 50000),
    (2, 102, 1000),
    (3, 999, 2000),
    (4, 103, 1500),
    (5, 888, 3000),
    (6, 104, 12000)
  )
)
```

The transaction structure is:

```text
(Transaction ID, Product ID, Amount)
```

Example:

```text
(1, 101, 50000)
```

means:

```text
Transaction ID = 1
Product ID     = 101
Amount         = 50000
```

### 5. Create an Accumulator

Create an accumulator to count invalid transactions.

```scala
val badRecords = sc.longAccumulator("Bad Records")
```

When an invalid transaction is found:

```scala
badRecords.add(1)
```

The final value can be accessed using:

```scala
badRecords.value
```

### 6. Why Use an Accumulator?

Spark processing is distributed across executors.

```text
                 Driver
                   |
       +-----------+-----------+
       |           |           |
   Executor 1  Executor 2  Executor 3
       |           |           |
       +-----------+-----------+
                   |
              Accumulator
                   |
                 Driver
```

An accumulator allows tasks to contribute to a shared counter.

For example:

```text
Executor 1 -> Bad record +1
Executor 2 -> Bad record +1
Executor 3 -> No bad record
```

The final accumulator value becomes:

```text
Bad Records = 2
```

### 7. Why Not Use a Normal Driver Variable?

A normal variable should not be used for distributed updates.

For example:

```scala
var badRecords = 0
```

Using:

```scala
badRecords += 1
```

inside distributed RDD processing does not provide a reliable distributed counter.

Spark executes tasks on executors, while the variable belongs to the driver process.

Therefore, use an accumulator:

```scala
val badRecords = sc.longAccumulator("Bad Records")
```

and:

```scala
badRecords.add(1)
```

### 8. Validate Transactions

Use the broadcast product master to validate each transaction.

```scala
val validTransactions = transactions.filter { transaction =>

  val productId = transaction._2

  if (broadcastProducts.value.contains(productId)) {
    true
  } else {
    badRecords.add(1)
    false
  }
}
```

The validation process is:

```text
Transaction
     |
     ↓
Extract Product ID
     |
     ↓
Check Broadcast Product Master
     |
     +-------------------+
     |                   |
   Exists             Not Found
     |                   |
   Valid              Invalid
     |                   |
     ↓                   ↓
Keep RDD          Accumulator + 1
```

### 9. Invalid Transactions

The transaction data contains:

```text
(3, 999, 2000)
(5, 888, 3000)
```

Product IDs `999` and `888` do not exist in the product master.

Therefore:

```text
999 -> Invalid
888 -> Invalid
```

The accumulator counts:

```text
Bad Records = 2
```

### 10. Display Valid Transactions

Use `collect()` to display the valid transactions:

```scala
validTransactions.collect().foreach { transaction =>
  println(transaction)
}
```

The valid transactions are:

```text
(1,101,50000)
(2,102,1000)
(4,103,1500)
(6,104,12000)
```

### 11. Display the Accumulator

After processing:

```scala
println("Bad Transactions: " + badRecords.value)
```

The result is:

```text
Bad Transactions: 2
```

## Distributed Processing Scenario

### Problem

A transaction system receives many transactions and needs to validate each transaction against a small product master table.

The product master is small:

```text
101 -> Laptop
102 -> Mouse
103 -> Keyboard
104 -> Monitor
```

The transaction data may be distributed across multiple Spark tasks.

### Solution

Use a Broadcast Variable for the product master:

```text
Product Master
      |
   Broadcast
      |
+-----+-----+-----+
|           |     |
Task 1    Task 2  Task 3
```

Use an Accumulator for invalid transaction counting:

```text
Task 1 -> +1
Task 2 -> +0
Task 3 -> +1
             |
             ↓
       Bad Records = 2
```

### Result

```text
Total Transactions = 6
Valid Transactions = 4
Bad Transactions   = 2
```

## Important Notes

### Broadcast

Broadcast variables are suitable for small read-only datasets.

```text
Driver -> Executors
```

They should not be used for large datasets that cannot reasonably be broadcast.

### Accumulator

Accumulators are useful for counters and metrics.

```text
Executors -> Accumulator -> Driver
```

They should generally be used for metrics rather than controlling the main computation logic.

### Normal Variables

Do not assume that modifying a normal driver variable inside an RDD transformation will update the driver's variable reliably.

Use Spark's distributed mechanisms instead.

## Optimization Scenario

### Problem

Every transaction needs access to the same product master table.

Without broadcast, the reference data may need to be serialized and sent with tasks repeatedly.

```text
Transaction Task 1 -> Product Master
Transaction Task 2 -> Product Master
Transaction Task 3 -> Product Master
```

### Solution

Broadcast the small product master:

```text
Product Master
      |
   Broadcast
      |
+-----+-----+-----+
|           |     |
Task 1    Task 2  Task 3
```

This allows Spark tasks to efficiently access the same read-only reference data.

### Important Note

Broadcasting is beneficial when the reference data is small enough to fit comfortably in executor memory.

The size of the broadcast data and available executor memory should always be considered.

## How to Run

Run the following commands from the project directory.

### Compile

```bash
sbt compile
```

### Run

```bash
sbt run
```

## Output

The program displays the valid transactions, invalid transactions, total transaction count, valid transaction count, and accumulator result.

Example output:

```text
========== VALID TRANSACTIONS ==========

Transaction ID: 1, Product ID: 101, Amount: 50000
Transaction ID: 2, Product ID: 102, Amount: 1000
Transaction ID: 4, Product ID: 103, Amount: 1500
Transaction ID: 6, Product ID: 104, Amount: 12000

========== SUMMARY ==========

Total Transactions : 6
Valid Transactions : 4
Bad Transactions   : 2
```

Bad transaction messages may also appear during processing:

```text
Bad Transaction -> ID: 3, Product: 999, Amount: 2000
Bad Transaction -> ID: 5, Product: 888, Amount: 3000
```

## Key Takeaways

```text
Broadcast
    ↓
Small read-only reference data
    ↓
Driver → Executors
```

```text
Accumulator
    ↓
Distributed counter
    ↓
Executors → Driver
```

The project demonstrates how Broadcast Variables and Accumulators can be combined with RDD processing to build a distributed transaction validation workflow.
