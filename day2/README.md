# Day2ScalaCollections

A beginner-friendly Scala project that demonstrates Scala Collections by processing a simple sales dataset without using Spark.

## Technologies Used

* Scala 2.12.18
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Scala Collections Covered

* List
* `map`
* `filter`
* `flatMap`
* `reduce`
* Vector
* Map
* For-comprehension
* `groupBy`
* `sum`

## Project Structure

```text
Day2ScalaCollections/
├── build.sbt
├── README.md
└── src/
    └── main/
        └── scala/
            └── Day2ScalaCollections.scala
```

## Practice Tasks

### 1. Process Sales List

Use a `List` of sales values and demonstrate:

* `map` to transform sales
* `filter` to select high-value sales
* `flatMap` to flatten sales data
* `reduce` to calculate total sales

### 2. Vector

Use `Vector` to store indexed customer records.

Demonstrate accessing customer records using an index and explain why `Vector` is useful for indexed access.

### 3. Map

Use `Map` to store:

* Product quantities
* Product prices

Calculate the total value of products using quantity and price.

### 4. For-Comprehension

Combine customer and order records using a for-comprehension by matching customer IDs.

### 5. Daily Sales Summary

Create a daily sales summary without Spark.

Use Scala Collections to:

* Calculate sales amounts
* Group sales by date
* Calculate daily totals
* Calculate the grand total

## How to Run

Run the following command from the project directory:

```bash
sbt run
```

## Output

The program displays:

```text
===== DAY 2: SCALA COLLECTIONS PRACTICE =====

Original Sales:
List(100, 250, 150, 300, 200)

Sales greater than 200:
List(250, 300)

===== VECTOR: CUSTOMER RECORDS =====

===== MAP: PRODUCT SALES =====

===== FOR-COMPREHENSION =====

===== DAILY SALES SUMMARY =====

Grand Total Sales: ₹...
```
