# Day 29 — End-to-End E-Commerce Analytics Pipeline

## Overview

This project builds a batch e-commerce analytics pipeline using Apache Spark and Scala. It ingests raw orders, validates and cleans records, enriches orders with customer and product reference data, computes customer-level metrics, and generates category and regional sales reports.

The project demonstrates both RDD-based and DataFrame-based processing, joins, SQL window functions, persistence, partitioning, Parquet storage, and Spark execution-plan analysis.

## Architecture

    +-----------------------+
    | Raw CSV Sources       |
    | Orders                |
    | Customers             |
    | Products              |
    +-----------+-----------+
                |
                v
    +-----------------------+
    | Raw Data Preservation |
    | output/raw            |
    +-----------+-----------+
                |
                v
    +-----------------------+
    | RDD Cleaning          |
    | Validation and Filter |
    +-----------+-----------+
                |
                v
    +-----------------------+
    | Clean Orders          |
    | Total Amount          |
    | output/clean          |
    +-----------+-----------+
                |
                +----------------------+
                |                      |
                v                      v
    +-----------------------+  +-----------------------+
    | Pair RDD Aggregation  |  | Broadcast Reference   |
    | Customer Spend        |  | Customers and Products|
    +-----------------------+  +-----------+-----------+
                                           |
                                           v
                               +-----------------------+
                               | DataFrame Enrichment  |
                               | Customer/Product Join |
                               +-----------+-----------+
                                           |
                                           v
                               +-----------------------+
                               | Window Analytics      |
                               | Running Customer Spend|
                               | Customer Order Rank   |
                               +-----------+-----------+
                                           |
                                           v
                               +-----------------------+
                               | Aggregate Reports     |
                               | Category and Region   |
                               +-----------+-----------+
                                           |
                                           v
                               +-----------------------+
                               | Parquet Outputs       |
                               | Enriched Data         |
                               | Window Metrics        |
                               | Sales Reports         |
                               +-----------------------+

## Technology Stack

- Apache Spark 3.5.9
- Scala 2.12.18
- SBT 1.11.7
- Spark SQL
- RDD and Pair RDD APIs
- Parquet
- Ubuntu/WSL and VS Code

## Project Structure

    spark-day29/
    ├── build.sbt
    ├── README.md
    ├── data/
    │   ├── orders.csv
    │   ├── customers.csv
    │   └── products.csv
    ├── src/
    │   └── main/
    │       └── scala/
    │           └── EcommercePipeline.scala
    └── output/
        ├── raw/
        ├── clean/
        ├── enriched/
        ├── customer_windows/
        ├── category_sales/
        └── regional_sales/

## Pipeline Stages

### 1. Raw Ingestion

Reads orders, customers, and products from CSV files with headers. The raw orders are also preserved in Parquet for auditing and reprocessing.

### 2. Cleaning and Validation

Uses an RDD transformation to validate required identifiers, numeric quantity and price, date format, and completed order status.

The pipeline excludes invalid records, cancelled orders, and orders with non-positive quantities.

### 3. Customer Spend Aggregation

Uses a Pair RDD keyed by customer ID and `reduceByKey` to calculate customer spending.

This demonstrates key-value aggregation and the shuffle required to combine values belonging to the same key.

### 4. Data Enrichment

Joins cleaned orders with customer and product reference data.

Broadcast joins are used because the supplied customer and product datasets are small. Inner joins exclude records that do not have matching reference data.

### 5. Window Analytics

Uses Spark SQL window functions to calculate running customer spend and the chronological order number for each customer.

### 6. Sales Aggregation

Generates category/month reports containing order count, units sold, revenue, and average order value. It also creates regional revenue reports.

### 7. Output Storage

Writes the results to Parquet. Enriched data and category sales are partitioned by category to support filtered downstream queries.

## Input Schemas

### Orders

| Field | Description |
|---|---|
| orderId | Order identifier |
| customerId | Customer reference |
| productId | Product reference |
| quantity | Number of units |
| unitPrice | Price per unit |
| orderDate | ISO-format order date |
| status | Order status |

### Customers

| Field | Description |
|---|---|
| customerId | Customer identifier |
| customerName | Customer name |
| region | Customer region |

### Products

| Field | Description |
|---|---|
| productId | Product identifier |
| productName | Product name |
| category | Product category |

## How to Run

From the project root:

    sbt compile
    sbt run

The application reads CSV files from `data/` and writes results to `output/`.

## Expected Sample Counts

- Raw orders: 15
- Clean completed orders: 12
- Enriched orders: 11

The counts reflect the supplied sample data, including one cancelled order, one invalid quantity, and one completed order referencing an unknown customer.

## Shuffle, Stage, and Optimization Analysis

### RDD Aggregation

`reduceByKey` shuffles keys so values for the same customer can be combined. Map-side combining can reduce data transferred across the network.

### Joins

Broadcast joins are appropriate for small reference datasets because they can avoid shuffling the large orders dataset for those joins. Broadcast should not be used indiscriminately for large tables.

### Window Functions

Window partitioning by customer generally requires redistribution and sorting. The cost grows with the volume and skew of the data.

### Grouped Aggregations

Grouping by category and month requires records with matching keys to be brought together, typically through a shuffle.

### Persistence

`MEMORY_AND_DISK` is used for clean and enriched datasets that are reused by multiple actions. Persistence has a storage cost and should be retained only when reuse justifies it.

### Partitioned Output

Partitioning by category can improve reads that filter on category. Excessive partition cardinality or small output files can hurt performance.

### UDF Strategy

No custom UDF is required in this project. Built-in Spark expressions are preferred because Spark can optimize them through Catalyst.

### Execution Plan

Use `explain(true)` on important DataFrames to inspect logical and physical plans. The Spark UI can be used to inspect jobs, stages, tasks, shuffles, and execution time.

## Production Considerations

Before production deployment, add:

- Explicit schemas and stricter malformed-record handling.
- Data-quality metrics and a rejected-record quarantine.
- Idempotent writes and controlled output paths.
- Incremental processing and orchestration.
- Monitoring, logging, and failure alerts.
- Data privacy and access controls.
- Tests for cleaning rules, joins, aggregations, and edge cases.
- A strategy for schema evolution and late-arriving data.
- Partition sizing and file compaction based on workload.
- Configuration-driven deployment rather than a hard-coded local master.

The included datasets are small learning samples. Production performance must be evaluated using representative data volumes and workload measurements.

## Final Status

This demonstrates an end-to-end e-commerce pipeline using RDDs, Pair RDDs, DataFrames, joins, SQL windows, persistence, Parquet, and partitioned analytical outputs.
