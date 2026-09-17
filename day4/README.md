# Spark Day 4 — RDD Creation

A beginner-friendly Scala Spark project demonstrating the creation and processing of RDDs using collections and text files.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Java 11.0.31
* Ubuntu Linux

## Project Structure

```text
spark-day4/
├── build.sbt
├── README.md
├── data/
│   └── transactions.txt
└── src/
    └── main/
        └── scala/
            └── SparkDay4.scala
```

## Day 4 Objectives

* Create RDDs from collections.
* Create RDDs from text files.
* Use `map()` on an RDD.
* Use `filter()` on an RDD.
* Use `flatMap()` on an RDD.
* Calculate total sales from transaction records.
* Inspect RDD partitions.
* Understand default parallelism.
* Demonstrate processing a large file using multiple partitions.

## What is an RDD?

RDD stands for **Resilient Distributed Dataset**.

It is a distributed collection of data that can be processed in parallel across partitions.

An RDD is divided into partitions:

```text
RDD
│
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
```

Each partition can be processed independently by Spark.

## Creating an RDD from a Collection

An RDD can be created from a Scala collection using `parallelize()`:

```scala
val numbersRDD = sc.parallelize(1 to 10)
```

## Creating an RDD from a Text File

Spark can read a text file and create an RDD using:

```scala
val transactionsRDD = sc.textFile("data/transactions.txt")
```

Each line of the file becomes an element of the RDD.

## RDD Transformations

### map()

`map()` transforms every element.

```scala
val squaredRDD = numbersRDD.map(number => number * number)
```

Example:

```text
1 → 1
2 → 4
3 → 9
4 → 16
```

`map()` generally produces one output element for each input element.

### filter()

`filter()` keeps only elements that satisfy a condition.

```scala
val evenRDD = numbersRDD.filter(number => number % 2 == 0)
```

Output:

```text
2
4
6
8
10
```

### flatMap()

`flatMap()` can produce multiple output elements from one input element.

Example:

```scala
val wordsRDD = sentencesRDD.flatMap(
  sentence => sentence.split(" ")
)
```

Input:

```text
"Spark is fast"
```

Output:

```text
Spark
is
fast
```

## Total Sales Calculation

The transaction file contains:

```text
TransactionID,Product,Quantity,Price
```

Example:

```text
101,Notebook,2,100
```

The sales amount is calculated as:

```text
Quantity × Price
```

The Spark code converts every transaction into a sales amount and uses `reduce()` to calculate the total:

```scala
val totalSales = transactionsRDD
  .map { line =>
    val fields = line.split(",")
    val quantity = fields(2).toInt
    val price = fields(3).toDouble
    quantity * price
  }
  .reduce(_ + _)
```

For the sample transaction data:

```text
Total Sales: 1220.00
```

## Partitions

An RDD is divided into partitions so Spark can process data in parallel.

The number of partitions can be inspected using:

```scala
transactionsRDD.getNumPartitions
```

The number of records in each partition can also be inspected using:

```scala
transactionsRDD
  .mapPartitions(iterator => Iterator(iterator.size))
  .collect()
```

## Default Parallelism

Spark provides:

```scala
sc.defaultParallelism
```

to determine the default level of parallelism for operations that use the default partitioning behavior.

When running locally with:

```scala
.master("local[2]")
```

two CPU cores are available for local execution, and the default parallelism is commonly 2.

The exact number of partitions for a file-based RDD can depend on the file size and how Spark splits the input.

## Large Customer File Scenario

A large customer file can be divided into multiple partitions:

```scala
val customerRDD = sc.textFile("data/transactions.txt", 4)
```

The second argument requests a minimum number of partitions.

Conceptually:

```text
Large Customer File
        │
        ├── Partition 0
        ├── Partition 1
        ├── Partition 2
        └── Partition 3
```

Spark can process these partitions in parallel.

## Running the Project

Compile:

```bash
sbt compile
```

Run:

```bash
sbt run
```

## Expected Output

The application demonstrates:

```text
RDD Created From Collection
map()
filter()
flatMap()
Transaction Records
Total Sales: 1220.00
Partition Information
Default Parallelism
Multiple Partition Scenario
```
