# Day5 Transformations & Actions

A beginner-friendly Apache Spark project that demonstrates RDD Transformations and Actions by analyzing application log data.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Spark RDD Concepts Covered

* RDD
* `map`
* `filter`
* `flatMap`
* `distinct`
* `union`
* `count`
* `collect`
* `first`
* `take`
* `reduce`
* Transformations vs Actions
* Lazy Evaluation

## Project Structure

```text
Day5TransformationsActions/
├── build.sbt
├── README.md
└── src/
    └── main/
        └── scala/
            └── LogAnalyzer.scala
```

## Practice Tasks

### 1. Map Transformation

Use `map` to extract the log level from each log message.

### 2. Filter Transformation

Use `filter` to select only `ERROR` messages.

This keeps only log messages beginning with `ERROR`.

### 3. FlatMap Transformation

Use `flatMap` to split log messages into individual words.

### 4. Distinct Transformation

Use `distinct` to remove duplicate log messages.

### 5. Union Transformation

Use `union` to combine two RDDs containing log messages.

`union` does not automatically remove duplicate records.

### 6. Count Action

Use `count` to find the total number of log messages.

### 7. Collect Action

Use `collect` to retrieve all log messages.

### 8. First Action

Use `first` to retrieve the first log message.

### 9. Take Action

Use `take` to retrieve the first few log messages.

### 10. Reduce Action

Use `reduce` to combine values into a single result.

### 11. Transformations vs Actions

Understand the difference between transformations and actions.

Transformations:

```text
map
filter
flatMap
distinct
union
```

Actions:

```text
count
collect
first
take
reduce
```

Transformations create new RDDs and are lazily evaluated.

Actions trigger Spark execution.

### 12. Lazy Evaluation

Understand that Spark transformations are not executed immediately.

For example:

```scala
val errors = combinedLogs.filter(_.startsWith("ERROR"))
```

The `filter` operation is lazy.

Execution occurs when an action is called:

```scala
val errorCount = errors.count()
```

### 13. Log Analyzer

Build a simple log analyzer that counts `ERROR` messages.

The main operation is:

```scala
val errorCount = combinedLogs
  .filter(_.startsWith("ERROR"))
  .count()
```

The process is:

```text
Log Data
   ↓
RDD
   ↓
filter()
   ↓
ERROR Logs
   ↓
count()
   ↓
Total ERROR Messages
```

## How to Run

Run the following commands from the project directory.

### Compile the project

```bash
sbt compile
```

### Run the project

```bash
sbt run
```

## Output

The program displays:

```text
========== MAP ==========

INFO
ERROR
INFO
WARNING
ERROR
INFO
ERROR

========== FILTER: ERROR LOGS ==========

ERROR Database connection failed
ERROR File not found
ERROR Database connection failed

========== FLATMAP: WORDS ==========

INFO
Application
started
ERROR
Database
connection
failed
INFO
User
logged
in
WARNING
Disk
space
is

========== DISTINCT LOGS ==========

INFO Application started
ERROR Database connection failed
INFO User logged in
WARNING Disk space is low
ERROR File not found
INFO Processing request

========== UNION ==========

INFO Application started
ERROR Database connection failed
INFO User logged in
WARNING Disk space is low
ERROR File not found
INFO Processing request
ERROR Database connection failed
INFO Application started
ERROR Network timeout
INFO User logged out
WARNING Memory usage high
ERROR File not found

========== ERROR COUNT ==========

Total ERROR messages: 5

========== COUNT ==========

Total logs: 12

========== COLLECT ==========

INFO Application started
ERROR Database connection failed
INFO User logged in
WARNING Disk space is low
ERROR File not found
INFO Processing request
ERROR Database connection failed
INFO Application started
ERROR Network timeout
INFO User logged out
WARNING Memory usage high
ERROR File not found

========== FIRST ==========

INFO Application started

========== TAKE 3 ==========

INFO Application started
ERROR Database connection failed
INFO User logged in

========== REDUCE ==========

Sum: 150

========== LAZY TRANSFORMATION ==========

filter() has created a new RDD.
No computation happens until an action is called.
Action count() triggered execution.
ERROR count: 5
```
