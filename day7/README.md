# Day7 Immutability Lineage

A beginner-friendly Apache Spark project that demonstrates RDD immutability, lineage, and fault tolerance using a multi-step RDD transformation chain.

The project also conceptually simulates executor loss and explains how Spark can recompute a lost partition using RDD lineage.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Spark Concepts Covered

* RDD Immutability
* RDD Lineage
* RDD Transformations
* Fault Tolerance
* Partition Recovery
* Executor Loss
* `map`
* `filter`
* `collect`
* `reduce`
* `count`
* `toDebugString`

## Practice Tasks

### 1. Multi-Step RDD Transformation Chain

Create an RDD containing numbers from 1 to 10.

```scala
val numbers = sc.parallelize(
  1 to 10,
  2
)
```

Apply multiple transformations:

```scala
val doubled = numbers.map(_ * 2)

val filtered = doubled.filter(_ > 10)

val squared = filtered.map(x => x * x)
```

The transformation chain is:

```text
numbers
   ↓
map(_ * 2)
   ↓
doubled
   ↓
filter(_ > 10)
   ↓
filtered
   ↓
map(x => x * x)
   ↓
squared
```

The final result is:

```text
144
196
256
324
400
```

### 2. RDD Immutability

RDDs are immutable.

A transformation does not modify the existing RDD. Instead, it creates a new RDD.

For example:

```scala
val doubled = numbers.map(_ * 2)
```

The original `numbers` RDD remains unchanged.

```text
numbers
1 2 3 4 5 6 7 8 9 10
```

The new `doubled` RDD contains:

```text
2 4 6 8 10 12 14 16 18 20
```

This allows Spark to maintain the dependency information between RDDs.

### 3. RDD Lineage

Lineage represents the sequence of transformations used to create an RDD.

The project uses:

```text
numbers
   ↓ map
doubled
   ↓ filter
filtered
   ↓ map
squared
```

Spark can display lineage information using:

```scala
squared.toDebugString
```

### 4. Fault Tolerance

RDDs provide fault tolerance through lineage.

If a partition is lost, Spark can use the lineage information to recompute the lost partition from the original data and the required transformations.

Conceptually:

```text
Lost squared partition
        ↓
Recompute filtered partition
        ↓
Recompute doubled partition
        ↓
Read required data from original numbers partition
        ↓
Recreate squared partition
```

Spark does not need to permanently store every intermediate RDD to recover the lost data.

### 5. Executor Loss Simulation

This project does not intentionally terminate a real Spark executor.

Instead, executor loss is demonstrated conceptually.

If an executor containing a partition fails:

```text
Executor Failure
       ↓
Partition Lost
       ↓
Spark detects missing partition
       ↓
Lineage is used
       ↓
Required transformations are recomputed
       ↓
Lost Partition Recreated
```

The project prints this recovery process to demonstrate the concept.

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

The program displays the original RDD, transformed RDDs, lineage information, RDD IDs, immutability demonstration, and conceptual fault-tolerance process.

Example output:

```text
========== ORIGINAL RDD ==========

1
2
3
4
5
6
7
8
9
10

========== DOUBLED RDD ==========

2
4
6
8
10
12
14
16
18
20

========== FILTERED RDD ==========

12
14
16
18
20

========== SQUARED RDD ==========

144
196
256
324
400

========== RDD LINEAGE ==========

...

========== RDD IDs ==========

Original RDD ID: 0
Doubled RDD ID: 1
Filtered RDD ID: 2
Squared RDD ID: 3

========== IMMUTABILITY ==========

Original RDD remains unchanged.

Original RDD:
1
2
3
4
5
6
7
8
9
10

Transformed RDD:
144
196
256
324
400

========== FAULT TOLERANCE ==========

If a partition of the final RDD is lost,
Spark uses lineage to recompute the lost partition.

Lineage:
numbers
   ↓ map(_ * 2)
doubled
   ↓ filter(_ > 10)
filtered
   ↓ map(x => x * x)
squared

Conceptual recomputation:
Lost squared partition
        ↓
Recompute filtered partition
        ↓
Recompute doubled partition
        ↓
Read required data from original numbers partition

========== FINAL RESULT ==========

Final record count: 5
Final sum: 1320
```

> **Note:** The exact `toDebugString` output and RDD IDs can vary between Spark runs. The transformation lineage and final results remain conceptually the same.

## Lineage Diagram

```text
                    numbers
                       |
                       | map(_ * 2)
                       ↓
                    doubled
                       |
                       | filter(_ > 10)
                       ↓
                   filtered
                       |
                       | map(x => x * x)
                       ↓
                    squared
```

## Fault-Tolerance Diagram

```text
             Original RDD
                  |
                  ↓
               doubled
                  |
                  ↓
              filtered
                  |
                  ↓
               squared
                  |
                  X
            Partition Lost
                  |
                  ↓
        Spark uses lineage
                  |
                  ↓
       Recompute dependencies
                  |
                  ↓
        Lost partition restored
```

## Key Learning

### Immutability

RDDs cannot be modified after creation. Transformations create new RDDs.

### Lineage

Spark keeps track of the transformations that produced an RDD.

### Fault Tolerance

If a partition is lost, Spark can recompute the required data using lineage.

### Executor Loss

An executor failure can cause its partitions to become unavailable. Spark can schedule the required computation again on another available executor.

