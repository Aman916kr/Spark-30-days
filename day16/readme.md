````
# Day 16 — Aggregations

## Objective
Practice Spark aggregation functions and generate hospital department revenue metrics using DataFrames.

## Concepts Covered
- `count()`, `sum()`, `avg()`, `min()`, `max()`
- `groupBy()` with multiple columns
- `agg()` for multiple aggregations
- HAVING-like filtering after aggregation
- Department-wise salary/revenue statistics

## Project Structure

```text
spark-day16/
├── build.sbt
├── data/
│   └── hospital_revenue.csv
└── src/
    └── main/
        └── scala/
            └── HospitalRevenueAnalysis.scala
````

## Dataset

`hospital_revenue.csv` contains hospital transaction details:

- Patient ID
- Department
- Doctor
- Service
- Revenue

## Implementation

### 1. Read Hospital Data

```
val hospitalDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/hospital_revenue.csv")
```

### 2. Overall Aggregations

Calculate total patients, total revenue, average revenue, minimum revenue, and maximum revenue.

```
hospitalDF.agg(
  count("patient_id").as("total_patients"),
  sum("revenue").as("total_revenue"),
  avg("revenue").as("average_revenue"),
  min("revenue").as("minimum_revenue"),
  max("revenue").as("maximum_revenue")
).show()
```

### 3. Department-wise Revenue

Group records by department and calculate revenue statistics.

```
hospitalDF
  .groupBy("department")
  .agg(
    count("patient_id").as("patient_count"),
    sum("revenue").as("total_revenue"),
    avg("revenue").as("average_revenue"),
    min("revenue").as("minimum_revenue"),
    max("revenue").as("maximum_revenue")
  )
  .show()
```

### 4. Multiple-column Grouping

Group by department and doctor to calculate each doctor's revenue contribution.

```
hospitalDF
  .groupBy("department", "doctor")
  .agg(
    count("patient_id").as("patient_count"),
    sum("revenue").as("total_revenue")
  )
  .show()
```

### 5. HAVING-like Filtering

Filter the aggregated department results to find departments with revenue greater than 50,000.

```
departmentStats
  .filter(col("total_revenue") > 50000)
  .show()
```

## How to Run

Run from the Ubuntu terminal:

```
cd ~/spark-30-days/spark-day16
sbt run
```

## Expected Output

### Overall Revenue Statistics

| Metric          | Value  |
| --------------- | ------ |
| Total patients  | 15     |
| Total revenue   | 319500 |
| Average revenue | 21300  |
| Minimum revenue | 3000   |
| Maximum revenue | 60000  |

### Department-wise Revenue

| Department  | Patients | Total Revenue |
| ----------- | -------- | ------------- |
| Cardiology  | 5        | 62000         |
| Oncology    | 3        | 121000        |
| Neurology   | 4        | 46500         |
| Orthopedics | 3        | 79500         |

### Departments with Revenue Above 50000

| Department  | Total Revenue |
| ----------- | ------------- |
| Oncology    | 121000        |
| Orthopedics | 79500         |
| Cardiology  | 62000         |

Expected values are based on the provided dataset.
