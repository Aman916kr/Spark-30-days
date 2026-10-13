# Day 25 — Window Operations in Apache Spark Streaming

## Objective
- Understand batch interval, window size, and sliding interval.
- Implement `countByWindow`.
- Implement `reduceByKeyAndWindow`.
- Calculate rolling sales totals.
- Detect a sudden increase in transactions within a 10-minute window.

## Technologies Used
- Apache Spark 3.5.9
- Scala 2.12.18
- SBT 1.11.7
- Java 11
- Spark Streaming
- Netcat
- Ubuntu / WSL
- VS Code

## Project Structure
```text
spark-day25/
├── build.sbt
├── checkpoint/
└── src/
    └── main/
        └── scala/
            └── WindowOperations.scala
```

## Concepts Covered

### 1. Batch Interval
The batch interval determines how frequently Spark Streaming processes incoming data.

In this project, the batch interval is 5 seconds.

### 2. Window Size
The window size specifies the duration of data included in a window calculation.

In this project, the window size is 10 minutes (600 seconds).

### 3. Sliding Interval
The sliding interval determines how frequently Spark updates the window result.

In this project, the sliding interval is 5 seconds.

### 4. Window Configuration
- Batch interval: 5 seconds
- Window size: 600 seconds
- Sliding interval: 5 seconds
- Number of batches in a full window: 120

## Implementation

### 1. Create Spark Streaming Context
Created a `StreamingContext` with a 5-second batch interval.

### 2. Configure Checkpointing
Configured checkpointing to support window operations and save streaming computation state.

### 3. Read Transactions
Read transaction records through a socket on `localhost:9999`.

Input format:

`transactionId,accountId,amount`

Example:

`T001,A101,1500`

### 4. Parse and Validate Transactions
Parsed transaction IDs, account IDs, and amounts. Invalid records and non-positive amounts were discarded.

### 5. Calculate Current-Batch Count
Used a batch-level count to determine the number of transactions received in each batch.

### 6. Calculate Rolling Transaction Count
Used `countByWindow` to calculate the number of transactions in the latest 10-minute window.

### 7. Calculate Rolling Sales
Used `reduceByKeyAndWindow` to aggregate sales by account over the latest 10-minute window.

### 8. Detect Transaction Spikes
Configured an alert threshold of 20 transactions within the 10-minute window.

When the rolling count reaches or exceeds 20, the application prints:

`ALERT: Transaction spike detected!`

## Commands to Run the Project

### Step 1: Navigate to the Project
```bash
cd ~/spark-30-days/spark-day25
```

### Step 2: Compile the Project
```bash
sbt compile
```

### Step 3: Start Netcat
Open a separate Ubuntu terminal:

```bash
nc -lk 9999
```

### Step 4: Run the Spark Application
In the project terminal:

```bash
sbt run
```

### Step 5: Send Test Transactions
Enter the following records in the Netcat terminal:

```text
T001,A101,1500
T002,A101,2500
T003,B201,1000
T004,B201,3000
T005,A101,2000
```

## Expected Output

The exact results depend on which batch receives each transaction.

Example:

```text
Current batch transaction count: 1

Rolling 10-minute transaction count: 3

===== ROLLING 10-MINUTE SALES BY ACCOUNT =====
A101 -> 4000.00
B201 -> 1000.00
```

The example above represents a particular point in the stream, not the final result for all five sample transactions.

When the rolling count reaches 20:

```text
ALERT: Transaction spike detected!
```

## Difference Between Batch and Window Operations

| Feature | Batch Count | Window Count |
|---|---|---|
| Data considered | Current batch | Recent time window |
| Duration | 5 seconds | 10 minutes |
| Updates | Every batch | Every sliding interval |
| Use case | Monitor incoming batch volume | Detect transaction spikes |

## Business Scenario
A banking system receives transactions from multiple accounts. The application calculates the number of recent transactions and rolling sales totals for each account. If transaction activity rises above the configured threshold, the application generates an alert for further investigation.

