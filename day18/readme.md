# Day 18 — Joins

## Objective
Learn how to join multiple datasets in Apache Spark using different join types, handle ambiguous column names with aliases, and manage null values after joins.

## Concepts Covered
- Inner Join
- Left Join
- Right Join
- Full Outer Join
- Table aliases to avoid ambiguous column names
- Null handling using `coalesce()`
- Three-table joins
- Understanding Shuffle Sort Merge Join using `explain(true)`

## Project Structure
```text
spark-day18/
├── build.sbt
├── data/
│   ├── orders.csv
│   ├── customers.csv
│   └── payments.csv
└── src/
    └── main/
        └── scala/
            └── JoinsAnalysis.scala
```

## Dataset
The project uses three CSV datasets:

- **orders.csv** — order details, customer ID, date, and amount.
- **customers.csv** — customer names and cities.
- **payments.csv** — payment details, status, and paid amount.

## Implementation

### 1. Inner Join
Returns only rows where the join condition matches in both datasets.

```scala
orders.join(
  customers,
  col("o.customer_id") === col("c.customer_id"),
  "inner"
)
```

### 2. Left Join
Returns all rows from the left dataset and matching rows from the right dataset. Unmatched right-side values become `NULL`.

```scala
orders.join(
  payments,
  col("o.order_id") === col("p.order_id"),
  "left"
)
```

### 3. Right Join
Returns all rows from the right dataset and matching rows from the left dataset.

```scala
orders.join(
  customers,
  col("o.customer_id") === col("c.customer_id"),
  "right"
)
```

### 4. Full Outer Join
Returns all matching and non-matching rows from both datasets.

```scala
orders.join(
  customers,
  col("o.customer_id") === col("c.customer_id"),
  "full"
)
```

### 5. Aliases and Ambiguous Columns
Both orders and customers contain `customer_id`. Aliases (`o` and `c`) identify which dataset a column belongs to.

```scala
orders.alias("o")
customers.alias("c")

col("o.customer_id")
col("c.customer_id")
```

### 6. Null Handling
After a left join, unmatched payment records contain null values. `coalesce()` replaces a null payment status with `"Not Paid"`.

```scala
coalesce(col("payment_status"), lit("Not Paid"))
```

### 7. Three-Table Join
Combines orders, customers, and payments to produce a complete order view containing customer information and payment details.

### 8. Shuffle Sort Merge Join
Spark can use Shuffle Sort Merge Join for joins involving large datasets. It shuffles records by join key, sorts the shuffled data, and merges matching keys.

Use `explain(true)` to inspect the query plan and see which join strategy Spark selects. The selected strategy depends on factors such as data size, statistics, and configuration.

## How to Run

Open the Ubuntu terminal and navigate to the project:

```bash
cd ~/spark-30-days/spark-day18
```

Run the application:

```bash
sbt run
```

## Expected Output
The application displays:
- Matching orders and customers from the inner join.
- All orders with payment details from the left join.
- All customers from the right join.
- Matching and unmatched records from the full outer join.
- A combined orders-customers-payments result.
- The physical and logical query plans for the join.

Unmatched payment details should appear as `NULL` before null handling. The displayed payment status should show `"Not Paid"` after applying `coalesce()`.
