````
# Day 17 — Window Functions

## Objective
Practice Spark window functions to rank students within courses, identify the latest policy for each customer, and compare records using `lag()` and `lead()`.

## Concepts Covered
- `row_number()`, `rank()`, and `dense_rank()`
- Window specifications using `Window.partitionBy()` and `orderBy()`
- Top 3 students per course
- Latest policy per customer
- `lag()` and `lead()` for previous and next records

## Project Structure

```text
spark-day17/
├── build.sbt
├── data/
│   ├── student_scores.csv
│   └── customer_policies.csv
└── src/
    └── main/
        └── scala/
            └── WindowFunctionsAnalysis.scala
````

## Datasets

### Student Scores

Contains student ID, name, course, score, and exam date.

### Customer Policies

Contains policy ID, customer ID, policy type, premium, and policy date.

## Implementation

### 1. Create a Window Specification

Partition students by course and order them by score in descending order.

```
val courseWindow = Window
  .partitionBy("course")
  .orderBy(desc("score"))
```

### 2. Apply Ranking Functions

Use `row_number()`, `rank()`, and `dense_rank()` to rank students within each course.

```
val rankedStudents = studentsDF
  .withColumn("row_number", row_number().over(courseWindow))
  .withColumn("rank", rank().over(courseWindow))
  .withColumn("dense_rank", dense_rank().over(courseWindow))
```

Difference:

- `row_number()` assigns a unique sequential number to each row.
- `rank()` gives tied values the same rank and leaves gaps afterward.
- `dense_rank()` gives tied values the same rank without leaving gaps.

### 3. Find Top 3 Students Per Course

```
rankedStudents
  .filter(col("row_number") <= 3)
  .orderBy("course", "row_number")
  .show()
```

### 4. Find Latest Policy Per Customer

Partition policies by customer and order them by policy date in descending order.

```
val latestPolicyWindow = Window
  .partitionBy("customer_id")
  .orderBy(desc("policy_date"), desc("policy_id"))

policiesDF
  .withColumn("row_number", row_number().over(latestPolicyWindow))
  .filter(col("row_number") === 1)
  .drop("row_number")
  .show()
```

### 5. Use lag() and lead()

Compare each policy's premium with the previous and next policy for the same customer.

```
val policyHistoryWindow = Window
  .partitionBy("customer_id")
  .orderBy("policy_date")

policiesDF
  .withColumn("previous_premium", lag("premium", 1).over(policyHistoryWindow))
  .withColumn("next_premium", lead("premium", 1).over(policyHistoryWindow))
  .show()
```

## How to Run

Run from the Ubuntu terminal:

```
cd ~/spark-30-days/spark-day17
sbt run
```

## Expected Output

### Top 3 Students Per Course

| Course     | Student | Score |
| ---------- | ------- | ----- |
| Databricks | Kate    | 91    |
| Databricks | Mia     | 91    |
| Databricks | Leo     | 87    |
| Scala      | Frank   | 95    |
| Scala      | Henry   | 95    |
| Scala      | Grace   | 89    |
| Spark      | Alice   | 92    |
| Spark      | Charlie | 92    |
| Spark      | Emma    | 88    |

For tied scores, `row_number()` assigns a sequential number, but the order between tied students is not guaranteed because the window ordering uses only score.

### Latest Policy Per Customer

| Customer | Latest Policy | Policy Date | Premium |
| -------- | ------------- | ----------- | ------- |
| C101     | P006          | 2025-04-12  | 14000   |
| C102     | P009          | 2026-01-10  | 11000   |
| C103     | P008          | 2025-08-30  | 18000   |
| C104     | P010          | 2026-03-15  | 16000   |

### Lag and Lead

The policy history output includes:

- `previous_premium`: premium from the previous policy for that customer.
- `next_premium`: premium from the next policy for that customer.
- `null` appears when a previous or next policy does not exist.
