# Day9 PairRDD

A beginner-friendly Apache Spark project that demonstrates Pair RDDs using key-value data and Spark operations such as `reduceByKey`, `groupByKey`, and `mapValues`.

The project analyzes bank transactions and calculates revenue by account, product, and department.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Pair RDD Concepts Covered

* Pair RDD
* Key-value RDDs
* `reduceByKey`
* `groupByKey`
* `mapValues`
* Revenue aggregation
* Account-level aggregation
* Department-level aggregation
* `reduceByKey` vs `groupByKey`
* Bank transaction analysis

## Practice Tasks

### 1. Create Key-Value RDDs

Create a Pair RDD from bank transaction data.

```scala
val accountAmounts = transactionRDD.map {
  case (accountId, department, product, amount) =>
    (accountId, amount)
}
```

The resulting Pair RDD contains:

```text
(Account ID, Transaction Amount)
```

Example:

```text
(A101,60000)
(A102,25000)
(A101,2000)
```

### 2. reduceByKey

Use `reduceByKey` to aggregate transactions belonging to the same account.

```scala
val accountTotals = accountAmounts
  .reduceByKey(_ + _)
```

For example:

```text
A101 → 60000
A101 → 2000
A101 → 3000
```

becomes:

```text
A101 → 65000
```

### 3. groupByKey

Use `groupByKey` to group all transaction amounts belonging to each account.

```scala
val accountGrouped = accountAmounts
  .groupByKey()
```

Example:

```text
A101 → 60000, 2000, 3000
A102 → 25000, 60000, 2000
```

### 4. mapValues

Use `mapValues` to transform only the values while keeping the keys unchanged.

```scala
val accountWithTax = accountTotals
  .mapValues(amount => amount * 0.10)
```

Example:

```text
A101 → 65000
```

becomes:

```text
A101 → 6500
```

### 5. Revenue by Product

Calculate total revenue for each product.

```scala
val productRevenue = transactionRDD
  .map {
    case (_, _, product, amount) =>
      (product, amount)
  }
  .reduceByKey(_ + _)
```

### 6. Revenue by Department

Calculate total revenue for each department.

```scala
val departmentRevenue = transactionRDD
  .map {
    case (_, department, _, amount) =>
      (department, amount)
  }
  .reduceByKey(_ + _)
```

### 7. reduceByKey vs groupByKey

The project compares the two approaches.

`reduceByKey` can perform local aggregation before shuffle:

```text
Input Values
     ↓
Local Combine
     ↓
Shuffle
     ↓
Final Result
```

`groupByKey` groups values after moving them through the shuffle:

```text
Input Values
     ↓
Shuffle
     ↓
Group Values
     ↓
Aggregation
```

For simple aggregations such as sum and count, `reduceByKey` is generally preferred because it can reduce the amount of data transferred during shuffle.

### 8. Bank Transaction Scenario

The main scenario is to aggregate bank transactions by account ID.

Input:

```text
Account ID → Amount
```

Example:

```text
A101 → 60000
A101 → 2000
A101 → 3000
```

Output:

```text
A101 → 65000
```

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

The program displays the Pair RDD, account totals, grouped transactions, revenue by product, revenue by department, performance timing, and total revenue.

Example output:

```text
========== KEY-VALUE RDD ==========

(A101,60000)
(A102,25000)
(A101,2000)
(A103,5000)
(A102,60000)
(A104,15000)
(A103,8000)
(A101,3000)
(A104,60000)
(A102,2000)

========== REDUCEBYKEY: ACCOUNT TOTALS ==========

(A101,65000)
(A102,87000)
(A103,13000)
(A104,75000)

========== GROUPBYKEY: ACCOUNT TRANSACTIONS ==========

A101 -> 60000, 2000, 3000
A102 -> 25000, 60000, 2000
A103 -> 5000, 8000
A104 -> 15000, 60000

========== MAPVALUES: ACCOUNT TAX ==========

(A101,6500.0)
(A102,8700.0)
(A103,1300.0)
(A104,7500.0)

========== REVENUE BY PRODUCT ==========

(Chair,5000)
(Keyboard,3000)
(Laptop,180000)
(Monitor,15000)
(Mouse,4000)
(Phone,25000)
(Table,8000)

========== REVENUE BY DEPARTMENT ==========

(HR,13000)
(IT,75000)
(Sales,152000)

========== PERFORMANCE COMPARISON ==========

reduceByKey time: <runtime-dependent> ms
groupByKey time:  <runtime-dependent> ms

reduceByKey performs local aggregation before shuffle.
groupByKey transfers all values for each key before aggregation.

========== BANK TRANSACTION ANALYSIS ==========

Transactions aggregated by Account ID:
A101 -> ₹65000
A102 -> ₹87000
A103 -> ₹13000
A104 -> ₹75000

========== TOTAL REVENUE ==========

Total revenue: ₹240000
```
