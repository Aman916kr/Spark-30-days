````
# Day 23 — DStreams Basics

## Objective
Learn the fundamentals of Spark Streaming using DStreams, read application logs from a TCP socket, process incoming data in micro-batches, and count ERROR messages at regular intervals.

## Concepts Covered
- Spark Streaming and DStreams
- StreamingContext
- Batch interval
- Socket-based streaming
- `flatMap()`
- `map()`
- `filter()`
- `reduceByKey()`
- Micro-batch processing
- Real-time application log analysis

## Project Structure
```text
spark-day23/
├── build.sbt
└── src/
    └── main/
        └── scala/
            └── LogStreaming.scala
````

## Implementation

### 1. Create StreamingContext

Create a Spark Streaming context with a five-second batch interval.

```
val ssc = new StreamingContext(conf, Seconds(5))
```

### 2. Read the Socket Stream

Read application log lines from a TCP socket on localhost port 9999.

```
val logStream = ssc.socketTextStream("localhost", 9999)
```

### 3. Process Log Messages

Split each log line into words using `flatMap()`.

```
val words = logStream.flatMap(_.split("\\s+"))
```

Convert words into key-value pairs using `map()`.

```
val wordPairs = words.map(word => (word, 1))
```

Filter only ERROR tokens.

```
val errors = wordPairs.filter {
  case (word, _) => word.equalsIgnoreCase("ERROR")
}
```

Count ERROR tokens within each batch.

```
val errorCounts = errors.reduceByKey(_ + _)
```

### 4. Start Streaming

Start the streaming computation and wait for termination.

```
ssc.start()
ssc.awaitTermination()
```

## How to Run

### Step 1: Start the socket server

Open a second Ubuntu terminal:

```
nc -lk 9999
```

If Netcat is not installed:

```
sudo apt update
sudo apt install netcat-openbsd
```

### Step 2: Run the Spark application

In the first Ubuntu terminal:

```
cd ~/spark-30-days/spark-day23
sbt run
```

### Step 3: Send test logs

In the socket terminal, type these lines and press Enter after each:

```
INFO Application started
ERROR Database connection failed
INFO Request completed
ERROR Payment service unavailable
ERROR Timeout occurred
```

## Expected Output

If two ERROR tokens arrive in the same five-second batch, the output will include:

```
========== ERROR COUNT: LAST 5-SECOND BATCH ==========
ERROR: 2
```

If no ERROR tokens arrive in a batch:

```
No ERROR messages in this batch.
```

The exact count depends on when the input lines arrive relative to the batch boundaries.

## Micro-Batch Processing

Spark Streaming divides incoming data into small time intervals. In this project, Spark processes data in successive five-second batches and counts ERROR tokens within each batch.

