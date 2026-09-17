# Day8 DAG Execution

A beginner-friendly Apache Spark project that demonstrates DAG construction and Spark execution using RDD transformations, actions, partitions, stages, tasks, and shuffle boundaries.

The project also analyzes a `reduceByKey` pipeline and predicts the number of stages created by the shuffle boundary.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Spark Concepts Covered

* DAG
* Jobs
* Stages
* Tasks
* Partitions
* Narrow transformations
* Wide transformations
* Shuffle
* `map`
* `filter`
* `reduceByKey`
* `collect`
* `count`
* RDD lineage
* Spark execution model

## Practice Tasks

### 1. Create a Multi-Step RDD Pipeline

Create an RDD with 4 partitions:

```scala
val numbers = sc.parallelize(1 to 20, 4)
```

Apply several transformations:

```scala
val doubled = numbers.map(_ * 2)

val filtered = doubled.filter(_ > 20)

val keyValueRDD = filtered.map { x =>
  (x % 3, x)
}

val reduced = keyValueRDD.reduceByKey(_ + _)

val finalResult = reduced.map {
  case (key, value) =>
    (key, value * 10)
}
```

The pipeline is:

```text
numbers
   ↓ map
doubled
   ↓ filter
filtered
   ↓ map
keyValueRDD
   ↓ reduceByKey
   ↓ SHUFFLE
reduced
   ↓ map
finalResult
```

### 2. DAG and Shuffle Boundary

A DAG represents the sequence of transformations that Spark uses to calculate the final result.

The important shuffle boundary in this project is created by:

```scala
reduceByKey(_ + _)
```

Conceptually:

```text
map
 ↓
filter
 ↓
map
 ↓
reduceByKey
     ↓
   SHUFFLE
     ↓
map
```

The shuffle separates the pipeline into stages.

### 3. Narrow Transformations

The following operations in this project are narrow transformations:

```text
map
filter
map
```

They can operate on data without requiring a full redistribution of records between partitions.

### 4. Wide Transformation

`reduceByKey` is a wide transformation because values with the same key need to be brought together.

```scala
val reduced = keyValueRDD.reduceByKey(_ + _)
```

This creates a shuffle boundary.

### 5. Jobs

A Spark job is triggered by an action.

This project uses:

```scala
finalResult.collect()
```

and:

```scala
finalResult.count()
```

Each action can trigger a separate Spark job.

### 6. Stages

Stages are created around shuffle boundaries.

For this pipeline:

```text
Stage 0
map
filter
map

       ↓ SHUFFLE

Stage 1
reduceByKey
map
```

Therefore, the conceptual stage prediction is:

```text
2 stages
```

### 7. Tasks

A task is the unit of work performed for one partition within a stage.

Conceptually:

```text
Stage
 │
 ├── Task 1 → Partition 1
 ├── Task 2 → Partition 2
 ├── Task 3 → Partition 3
 └── Task 4 → Partition 4
```

The number of tasks is related to the number of partitions being processed by a stage.

### 8. Partitions

The input RDD is created with four partitions:

```scala
val numbers = sc.parallelize(1 to 20, 4)
```

Therefore:

```text
Number of partitions = 4
```

Partitions allow Spark to process data in parallel.

## Narrow vs Wide Transformations

| Operation     | Type   | Shuffle |
| ------------- | ------ | ------- |
| `map`         | Narrow | No      |
| `filter`      | Narrow | No      |
| `map`         | Narrow | No      |
| `reduceByKey` | Wide   | Yes     |
| `map`         | Narrow | No      |

## Spark Execution Model

The basic Spark execution hierarchy is:

```text
Action
   ↓
Job
   ↓
Stage
   ↓
Task
   ↓
Partition
```

For the project:

```text
collect()
    ↓
   Job
    ↓
 ┌───────────────┐
 │    Stage 0    │
 │ map/filter/map│
 └───────────────┘
        ↓
     Shuffle
        ↓
 ┌───────────────┐
 │    Stage 1    │
 │ reduceByKey/map│
 └───────────────┘
        ↓
      Tasks
        ↓
   Partitions
```

## Stage Prediction Scenario

### Pipeline

```text
map
 ↓
filter
 ↓
map
 ↓
reduceByKey
 ↓
map
```

There is one shuffle boundary:

```text
map
 ↓
filter
 ↓
map
 ↓
reduceByKey
       ↓
    SHUFFLE
       ↓
map
```

Therefore the conceptual stage prediction is:

```text
Stage 0 → map → filter → map
Stage 1 → reduceByKey → map
```

### Predicted Number of Stages

```text
2 stages
```

The key idea is that the shuffle created by `reduceByKey` separates the stages.

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

The program displays the number of partitions, RDD lineage, result of the transformations, stage analysis, and Spark execution concepts.

Example output:

```text
========== PARTITIONS ==========

Number of partitions: 4

========== RDD LINEAGE ==========

...

========== ACTION: COLLECT ==========

(0,840)
(1,840)
(2,900)

========== ACTION: COUNT ==========

Result count: 3

========== STAGE ANALYSIS ==========

numbers
   ↓ map
doubled
   ↓ filter
filtered
   ↓ map
keyValueRDD
   ↓ reduceByKey
SHUFFLE BOUNDARY
   ↓
reduced
   ↓ map
finalResult

Predicted stages for each action: 2
Stage 0: map → filter → map
Stage 1: reduceByKey → map

========== NARROW VS WIDE ==========

map       -> Narrow
filter    -> Narrow
map       -> Narrow
reduceByKey -> Wide
map       -> Narrow

========== SPARK EXECUTION MODEL ==========

Action creates a Job.
Job is divided into Stages.
Stages are divided into Tasks.
Tasks operate on Partitions.
```
