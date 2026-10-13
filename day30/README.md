# Day 30 — E-Commerce Data Engineering Capstone

## 1. Project Overview

This capstone implements an end-to-end e-commerce data pipeline using Apache Spark and Scala. It combines batch analytics with a real-time streaming component.

The batch pipeline reads raw orders and reference data, validates records, calculates customer spending, enriches orders through joins, performs SQL window analytics, and writes Parquet reports.

The streaming pipeline receives order events over a TCP socket, applies event-time processing, and calculates product-level sales within one-minute windows.

## 2. Architecture Diagram

```
+--------------------+       +----------------------+
| Orders CSV         |       | Customers CSV        |
+---------+----------+       +----------------------+
          |                            |
          v                            |
+--------------------+                 |
| Preserve Raw Data  |                 |
+---------+----------+                 |
          |                            |
          v                            |
+--------------------+                 |
| RDD Cleaning       |                 |
| Accumulator        |                 |
+---------+----------+                 |
          |                            |
          v                            |
+--------------------+       +----------------------+
| Clean Orders       |       | Products CSV         |
| Cache / Persist    |       +----------------------+
+---------+----------+                 |
          |                            |
          +-------------+--------------+
                        |
                        v
             +----------------------+
             | Broadcast Joins      |
             | Customer / Product   |
             +----------+-----------+
                        |
                        v
             +----------------------+
             | Pair RDD Aggregation |
             | Customer Spend       |
             +----------------------+
                        |
                        v
             +----------------------+
             | SQL Windows and UDF  |
             | Running Spend / Rank |
             +----------+-----------+
                        |
                        v
             +----------------------+
             | Category Sales       |
             | Customer Reports     |
             | Parquet Outputs      |
             +----------------------+

Real-Time Branch
+--------------------+
| TCP Socket Events  |
+---------+----------+
          |
          v
+--------------------+
| Parse and Validate |
+---------+----------+
          |
          v
+--------------------+
| Event-Time Window  |
| Watermark          |
+---------+----------+
          |
          v
+--------------------+
| Product Sales      |
| Console Report     |
+--------------------+
```

## 3. Technology Stack

* Apache Spark 3.5.9
* Scala 2.12.18
* SBT 1.11.7
* Spark SQL
* RDD and Pair RDD APIs
* Structured Streaming
* Parquet
* Ubuntu/WSL

## 4. Project Structure

```
spark-day30/
├── build.sbt
├── README.md
├── data/
│   ├── orders.csv
│   ├── customers.csv
│   └── products.csv
├── src/
│   └── main/
│       └── scala/
│           └── EcommerceCapstone.scala
├── output/
│   ├── raw_orders/
│   ├── clean_orders/
│   ├── category_report/
│   └── customer_windows/
└── checkpoints/
    └── streaming/
```

## 5. Batch Pipeline

### Raw Ingestion

Reads CSV order, customer, and product datasets. Raw orders are preserved as Parquet for audit and reprocessing.

### Cleaning

An RDD parses and validates order identifiers, quantities, prices, dates, and status values. Invalid records are counted with a Spark accumulator.

Cancelled orders are excluded from completed-order analytics.

### Pair RDD Aggregation

A Pair RDD keyed by customer ID calculates customer spending with `reduceByKey`.

### Enrichment

Broadcast joins add customer and product details to completed orders. Inner joins exclude orders whose customer or product reference does not match.

### SQL Window Analytics

Window functions calculate running customer spending and chronological order rank.

### UDF

A Scala UDF classifies each order into HIGH, MEDIUM, or LOW value bands. Built-in Spark expressions should be preferred when they can implement the same logic.

### Aggregations

The pipeline calculates category-level order counts, units sold, and revenue, then writes reports to Parquet.

## 6. Streaming Pipeline

The Structured Streaming component reads events from a TCP socket. Each event contains:

```
eventId,customerId,productId,quantity,unitPrice,eventTime
```

It validates basic fields, applies a two-minute event-time watermark, and aggregates units sold and revenue per product in one-minute windows.

The console sink displays updated aggregates.

This socket source is intended for local practice. It is not a durable event broker and should be replaced by Kafka or another persistent source for a production system.

## 7. Execution Instructions

Compile:

```
sbt compile
```

Start the input socket in Terminal 1:

```
nc -lk 9999
```

Run the application in Terminal 2:

```
cd ~/spark-30-days/spark-day30
sbt run
```

Send sample streaming events:

```
EVT001,C101,P101,2,500,2026-10-13T10:00:01
EVT002,C102,P102,1,1200,2026-10-13T10:00:10
EVT003,C101,P101,3,500,2026-10-13T10:00:20
```

Stop the application with Ctrl+C after testing.

## 8. Shuffle, Stage, and Optimization Decisions

### Shuffles

`reduceByKey` shuffles keys to aggregate customer spending. Grouped aggregations redistribute records by their grouping keys. SQL window functions generally redistribute and sort records by the window partition and ordering keys.

### Stages

Spark commonly divides execution into stages at shuffle boundaries. Narrow transformations can be pipelined within a stage. The exact number of stages depends on the execution plan and Spark configuration.

### Broadcast

Broadcasting small customer and product tables can avoid shuffling the large order dataset for those joins. Broadcasting large datasets may cause excessive memory use.

### Persistence

`MEMORY_AND_DISK` allows clean and enriched datasets to be reused across actions. Persistence has a memory and disk cost, so cached datasets are released after their final use.

### Accumulator

The accumulator counts invalid records during the cleaning action. Accumulators are suitable for monitoring counters, not as the source of truth for business data. Retries and recomputation must be considered when interpreting accumulator values.

### Partitioning

`repartition(4, category)` redistributes records and introduces a shuffle. Four partitions are only a demonstration setting. Production partition counts should be selected using data volume, executor resources, skew, and measured task duration.

### UDF

The UDF demonstrates custom business logic, but built-in SQL functions are usually easier for Catalyst to optimize.

### Physical Plan

The application calls `explain(true)` to display logical and physical plans. Look for `Exchange`, broadcast joins, sort operators, and window operations.

## 9. DAG, Lineage, Executors, and YARN

### DAG

A directed acyclic graph represents the dependencies between Spark transformations. Spark uses the dependency graph to plan execution.

### Lineage

Lineage records how an RDD was derived from its parent RDDs. Spark can use lineage to recompute lost partitions.

### Stages and Tasks

A job is divided into stages, often at shuffle boundaries. A stage is divided into tasks that process individual partitions.

### Executors

Executors run tasks and can cache data in memory or on disk. Driver and executor responsibilities are distinct: the driver coordinates the application, while executors process partitions.

### YARN Deployment

For YARN deployment, use a compatible Spark installation and Hadoop/YARN cluster. Remove the hard-coded local master from the application before submitting it to YARN.

## 10. Production Improvements

* Replace socket input with Kafka or another durable source.
* Define explicit schemas and quarantine malformed records.
* Add automated unit and integration tests.
* Implement structured logging and pipeline metrics.
* Configure reliable checkpoint and output locations.
* Add idempotent writes and replay handling.
* Tune partitions using representative workloads.
* Monitor data skew, shuffle spill, and small files.
* Add access controls and data privacy safeguards.
* Package and test deployment in a real cluster before claiming YARN readiness.

## 11. Final Status

This capstone demonstrates batch and streaming processing, RDDs, Pair RDDs, accumulators, broadcast joins, persistence, a UDF, SQL joins, aggregations, windows, partition tuning, Parquet output, and event-time streaming.
