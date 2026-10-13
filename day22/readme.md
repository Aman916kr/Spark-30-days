Day 22 code is provided. Next, create the README in your `spark-day22` project.

Run:

```
cd ~/spark-30-days/spark-day22
nano README.md
```

Paste the following complete README:

````
# Day 22 — Batch Mini Project: E-commerce Daily Sales Pipeline

## Objective
Build an end-to-end Apache Spark batch pipeline to read raw e-commerce transactions, clean invalid records, join customer and product data, aggregate daily revenue, and write partitioned Parquet output.

## Concepts Covered
- Batch processing with Spark
- Reading CSV datasets
- Data cleaning and validation
- Removing duplicate transactions
- Joining customer and product master data
- Revenue calculation
- Daily sales aggregation
- Partitioned Parquet output
- Output verification

## Project Structure
```text
spark-day22/
├── build.sbt
├── data/
│   ├── transactions.csv
│   ├── customers.csv
│   └── products.csv
├── output/
│   └── daily_sales/
└── src/
    └── main/
        └── scala/
            └── EcommerceBatchPipeline.scala
````

## Dataset

### transactions.csv

Contains transaction ID, customer ID, product ID, transaction date, quantity, unit price, and transaction status.

### customers.csv

Contains customer ID, customer name, and city.

### products.csv

Contains product ID, product name, and category.

## Pipeline Implementation

### 1. Read Raw Transactions

Read transaction data using Spark DataFrame APIs.

### 2. Clean Invalid Records

The pipeline:

- Removes records with missing required fields.
- Removes transactions with invalid dates.
- Excludes zero or negative quantities and prices.
- Keeps only transactions with `COMPLETED` status.
- Removes duplicate transaction IDs.

### 3. Calculate Revenue

Calculate revenue for each transaction:

```
.withColumn("revenue", col("quantity") * col("unit_price"))
```

### 4. Join Customer and Product Data

Use inner joins to enrich transactions with customer and product information.

```
.join(customers, Seq("customer_id"), "inner")
.join(products, Seq("product_id"), "inner")
```

Transactions with customer or product IDs missing from the master datasets are excluded.

### 5. Aggregate Daily Sales

Group records by transaction date, city, and product category. Calculate:

- Total transactions
- Total quantity
- Total revenue

### 6. Write Partitioned Parquet

Write the daily sales summary partitioned by year, month, and day.

```
dailySales.write
  .mode("overwrite")
  .partitionBy("year", "month", "day")
  .parquet("output/daily_sales")
```

## Expected Output

### Pipeline Record Counts

| Stage                            | Records |
| -------------------------------- | ------- |
| Raw transactions                 | 18      |
| After cleaning                   | 14      |
| After customer and product joins | 13      |

### Revenue Summary

| Metric              | Expected Value |
| ------------------- | -------------- |
| Total quantity sold | 28             |
| Total revenue       | 27,500         |

### Partitioned Output Structure

```
output/daily_sales/
└── year=2026/
    └── month=10/
        ├── day=1/
        ├── day=2/
        ├── day=3/
        ├── day=4/
        ├── day=5/
        └── day=6/
```

Each date directory can contain one or more Parquet part files depending on Spark's partitioning.

## How to Run

Open the Ubuntu terminal:

```
cd ~/spark-30-days/spark-day22
sbt run
```

