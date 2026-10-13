# Day 24 — Stateless vs Stateful Streaming

## Objective

Implement stateless and stateful processing using Apache Spark Streaming (DStreams) to maintain running transaction counts for each bank account.

## Concepts Covered

- Stateless transformations
- Stateful transformations
- Micro-batch processing
- `reduceByKey()`
- `updateStateByKey()`
- `StreamingContext`
- Checkpointing
- Running counts by key
- Current-batch versus accumulated results

## Project Structure

    spark-day24/
    ├── build.sbt
    ├── checkpoint/
    └── src/
        └── main/
            └── scala/
                └── BankTransactionStreaming.scala

## Technologies Used

- Apache Spark 3.5.9
- Scala 2.12.18
- SBT 1.11.7
- Java 11
- Netcat
- Ubuntu on WSL

## Application Scenario

Simulate bank transactions arriving continuously through a TCP socket. Each transaction contains an account ID and transaction ID.

Example input:

    A101,T001
    A101,T002
    B201,T003

The application processes transactions in five-second micro-batches and displays both current-batch transaction counts and accumulated running counts.

## Implementation

### 1. Spark Configuration

Create a Spark configuration using local execution mode and initialize a StreamingContext with a five-second batch interval.

### 2. Socket Input Stream

Read transaction records from localhost on port 9999 using `socketTextStream()`.

### 3. Parse Transactions

Split each input line using a comma and extract the account ID and transaction ID.

### 4. Stateless Processing

Use `map()` to convert each transaction into an account and count pair. Use `reduceByKey()` to count transactions per account within each batch.

Stateless processing considers the records in the current batch only. It does not retain previous batch counts.

### 5. Stateful Processing

Use `updateStateByKey()` to maintain a running transaction count for every account.

The new count is calculated by adding the number of transactions received in the current batch to the account's previous accumulated count.

### 6. Checkpointing

Configure checkpointing using:

    ssc.checkpoint("checkpoint")

Checkpointing allows Spark Streaming to save information needed to recover stateful computations.

### 7. Display Results

Print the current-batch counts and accumulated running counts separately so their differences can be observed.

## Build Configuration

The `build.sbt` file uses Scala 2.12.18 and the Spark Streaming 3.5.9 dependency.

## How to Run

### Step 1: Open Terminal 1

Navigate to the project directory:

    cd ~/spark-30-days/spark-day24

Start Netcat:

    nc -lk 9999

If Netcat is not installed:

    sudo apt update
    sudo apt install netcat-openbsd

Keep this terminal open.

### Step 2: Open Terminal 2

Navigate to the project directory:

    cd ~/spark-30-days/spark-day24

Start the application:

    sbt run

Wait until Spark starts and connects to localhost:9999.

### Step 3: Send Test Transactions

Enter the following lines in Terminal 1, pressing Enter after each line:

    A101,T001
    A101,T002
    B201,T003

Wait for Spark to display the batch results.

Then send another group:

    A101,T004
    B201,T005
    A101,T006

## Expected Output

If the first three transactions are processed in the same batch:

    ===== CURRENT BATCH COUNTS =====
    A101 -> 2
    B201 -> 1

    ===== ACCUMULATED RUNNING COUNTS =====
    A101 -> 2
    B201 -> 1

If the next three transactions are processed together:

    ===== CURRENT BATCH COUNTS =====
    A101 -> 2
    B201 -> 1

    ===== ACCUMULATED RUNNING COUNTS =====
    A101 -> 4
    B201 -> 2

Actual output depends on the timing of transaction arrival and batch boundaries. If no new transactions arrive during a batch, the current-batch output can display:

    No transactions in this batch.

The accumulated counts should retain the previous state for accounts already seen.

## Stateless vs Stateful Processing

| Feature | Stateless | Stateful |
|---|---|---|
| Processing | Current batch | Current and previous batches |
| State retention | No | Yes |
| Transformation | `reduceByKey()` | `updateStateByKey()` |
| Example | Transactions received in five seconds | Total transactions per account |
| Checkpointing | Not required for this count alone | Configured for state recovery |

## Important Notes

- Start Netcat before running the Spark application.
- Keep both terminals open while testing.
- Enter one transaction per line.
- The transaction ID is used to illustrate transaction records; this implementation counts records and does not deduplicate transaction IDs.
- The current-batch count may differ depending on when records arrive.
- The checkpoint directory is generated at runtime and does not need to be created manually.
- Netcat is used as a local testing source, not as a production streaming ingestion system.
- Spark Streaming DStreams is an older streaming API; Spark Structured Streaming is generally preferred for new applications.

