# Day 27 — Real-Time Banking Transaction Monitoring with Apache Spark

## Overview

This project implements a real-time banking transaction monitoring application using Apache Spark Streaming and Scala. It processes transaction events in micro-batches, aggregates transactions by account, identifies suspicious transaction bursts, enriches transactions with branch risk information, and demonstrates caching and partitioning.

## Objectives

* Design and validate a banking transaction event schema.
* Aggregate transaction counts and amounts by account.
* Detect suspicious transaction bursts within a sliding time window.
* Enrich transactions using broadcast branch-risk reference data.
* Apply persistence and repartitioning.
* Understand how Spark applications can be deployed on YARN.

## Technologies Used

* Apache Spark 3.5.9
* Scala 2.12.18
* SBT 1.11.7
* Spark Streaming
* Netcat
* Ubuntu on WSL
* VS Code

## Project Structure

```
spark-day27/
├── build.sbt
├── README.md
├── checkpoint/
└── src/
    └── main/
        └── scala/
            └── BankingMonitoring.scala
```

## Transaction Event Schema

Each input transaction follows this comma-separated format:

```
transactionId,timestamp,accountId,amount,branchId,transactionType
```

| Field           | Description                   |
| --------------- | ----------------------------- |
| transactionId   | Unique transaction identifier |
| timestamp       | Transaction timestamp         |
| accountId       | Customer account identifier   |
| amount          | Positive transaction amount   |
| branchId        | Branch identifier             |
| transactionType | DEPOSIT or WITHDRAWAL         |

Example:

```
TXN101,2026-10-13T10:00:00,ACC100,6000,BR001,WITHDRAWAL
```

## Implementation

### 1. Real-Time Input

Spark receives transaction events from a TCP socket on port 9999.

### 2. Data Validation

The application checks field count, non-empty identifiers, valid positive amounts, and supported transaction types. Invalid records are discarded.

### 3. Account Aggregation

Transactions are grouped by account to calculate:

* Number of transactions.
* Total transaction amount.

### 4. Suspicious Burst Detection

A 60-second window updated every 5 seconds counts transactions per account. An account is flagged when its count reaches at least three transactions in the window.

This is a demonstration rule, not a production fraud-detection model.

### 5. Branch Risk Enrichment

A broadcast map associates branch IDs with risk levels:

* BR001 — HIGH
* BR002 — LOW
* BR003 — MEDIUM

Transactions are enriched with the corresponding risk level. Transactions from HIGH-risk branches with amounts of at least 5000 generate additional alerts.

The branch data and thresholds are illustrative.

### 6. Persistence

Transactions use MEMORY_AND_DISK persistence so Spark can reuse cached data without recomputing it unnecessarily.

### 7. Partitioning

The application repartitions transaction data into four partitions to demonstrate parallel processing. Four is a learning configuration, not necessarily the optimal production setting.

## How to Run

### Step 1: Compile

From the project directory:

```
sbt compile
```

### Step 2: Start the Input Socket

Open a separate Ubuntu terminal:

```
nc -lk 9999
```

Keep it running.

### Step 3: Start the Spark Application

Open another terminal:

```
cd ~/spark-30-days/spark-day27
sbt run
```

### Step 4: Send Test Transactions

Enter these lines in the Netcat terminal:

```
TXN101,2026-10-13T10:00:00,ACC100,6000,BR001,WITHDRAWAL
TXN102,2026-10-13T10:00:05,ACC100,7500,BR001,WITHDRAWAL
TXN103,2026-10-13T10:00:10,ACC100,9000,BR001,WITHDRAWAL
TXN104,2026-10-13T10:00:15,ACC200,1200,BR002,DEPOSIT
```

Stop the application with Ctrl+C when testing is complete.

## Expected Results

The application should produce output containing:

* Account-level transaction counts and totals.
* Suspicious-burst alerts for accounts with at least three transactions in the last 60 seconds.
* Branch risk classifications.
* High-risk transaction alerts.
* Partition count information.

Exact output order and timing may vary because the application uses micro-batches and multiple partitions.

## Key Concepts Learned

* Spark Streaming and micro-batch processing.
* Transaction event parsing and validation.
* Pair RDDs and reduceByKey.
* Sliding-window aggregation.
* Broadcast variables.
* MEMORY_AND_DISK persistence.
* RDD repartitioning.
* Basic risk-based transaction monitoring.

## YARN Deployment Notes

The current application is configured for local execution using `setMaster("local[*]")`.

Before deploying to YARN, remove the hard-coded local master configuration and package the application as a JAR. Submit it to a configured Hadoop/YARN cluster using `spark-submit --master yarn`. The cluster must have compatible Spark, Scala, and Java configurations, and the application must have access to its input source and checkpoint location.

The current socket-based input setup is intended for local learning. A distributed deployment requires a reachable socket service or a production streaming source such as Kafka.

## Limitations

* Uses a TCP socket rather than durable event storage.
* Uses illustrative branch-risk data and thresholds.
* Does not implement a production fraud-detection model.
* Uses local execution rather than a validated YARN deployment.
* Uses in-memory broadcast reference data rather than a persistent reference database.

