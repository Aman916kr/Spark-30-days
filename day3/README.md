# Spark Day 3 — Spark Setup and First Application

A beginner-friendly Scala Spark project that demonstrates Spark setup and the fundamentals of running a Spark application in local mode.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Java 11.0.31
* Ubuntu Linux

## Project Structure

```text
spark-day3/
├── build.sbt
├── README.md
├── data/
│   └── input.txt
└── src/
    └── main/
        └── scala/
            └── SparkDay3.scala
```

## Day 3 Objectives

* Create a Scala Spark project using SBT.
* Create a SparkSession.
* Create and use a SparkContext.
* Create an RDD from a collection.
* Read a text file using Spark.
* Display the contents of the text file.
* Understand Driver, Executor and Cluster Manager.
* Run the same application using different numbers of local cores.

## SparkSession

`SparkSession` is the main entry point for a Spark application.

```scala
val spark = SparkSession.builder()
  .appName("Spark Day 3")
  .master("local[2]")
  .getOrCreate()
```

It provides the interface required to work with Spark functionality such as DataFrames, SQL and Spark execution.

## SparkContext

`SparkContext` is the connection between the Spark application and the Spark execution environment.

In this project it is obtained from the SparkSession:

```scala
val sc = spark.sparkContext
```

It is used to create and work with RDDs.

Example:

```scala
val numbers = sc.parallelize(1 to 10)
```

## Reading a Text File

Spark can read a text file and create an RDD using `textFile()`.

```scala
val lines = sc.textFile("data/input.txt")
```

The contents can then be displayed using:

```scala
lines.collect().foreach(println)
```

## Spark Architecture

### Driver

The Driver is the main process of a Spark application.

It:

* Runs the main application code.
* Creates the SparkSession and SparkContext.
* Creates the execution plan.
* Requests resources.
* Sends tasks to executors.
* Collects results.

### Executor

Executors are processes that perform the actual computation.

They:

* Execute Spark tasks.
* Process data partitions.
* Store cached data.
* Return results to the Driver.

### Cluster Manager

A Cluster Manager manages resources in a Spark cluster.

Common cluster managers include:

* Standalone
* YARN
* Kubernetes

For this project, Spark is running in local mode, so a separate cluster manager such as YARN is not required.

## Local Mode

Spark can be run locally by specifying the number of CPU cores.

### Using 2 Cores

```scala
.master("local[2]")
```

This allows Spark to use 2 local CPU cores.

Run:

```bash
sbt run
```

### Using 4 Cores

Change:

```scala
.master("local[2]")
```

to:

```scala
.master("local[4]")
```

Then run:

```bash
sbt run
```

This allows Spark to use 4 local CPU cores.

### Comparison

| Configuration | Meaning                             |
| ------------- | ----------------------------------- |
| `local[2]`    | Run Spark locally using 2 CPU cores |
| `local[4]`    | Run Spark locally using 4 CPU cores |

The application logic remains the same; only the available local parallelism changes.

## Running the Project

Clone the repository and enter the project directory:

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd spark-day3
```

Compile the project:

```bash
sbt compile
```

Run the application:

```bash
sbt run
```

## Expected Output

The application displays:

```text
=== Spark Day 3 ===
Spark Version: 3.5.9
Application Name: Spark Day 3
Master: local[2]

=== Numbers RDD ===
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

=== Text File Contents ===
Aman completed Spark Day 3
Scala is used for Spark applications
Spark can process large datasets
RDDs are distributed collections
Spark supports local and cluster execution
```
