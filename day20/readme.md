````
# Day 20 — File Formats and Output

## Objective
Learn how to read and write CSV, JSON, and Parquet files using Apache Spark, partition output data by date, and understand how repartitioning affects file output.

## Concepts Covered
- Reading CSV, JSON, and Parquet
- Writing CSV, JSON, and Parquet
- `repartition()`
- `partitionBy()`
- Date-based partitioning
- File layout and output files
- Number of output files
- Parquet format

## Project Structure
```text
spark-day20/
├── build.sbt
├── data/
│   └── daily_sales.csv
└── src/
    └── main/
        └── scala/
            └── FileFormatsAnalysis.scala
````

## Dataset

The `daily_sales.csv` dataset contains daily sales records with the following columns:

- sale_id
- product
- region
- sale_date
- quantity
- amount

The application derives year, month, and day from the sale date.

## Implementation

### 1. Read CSV

Read the source dataset with headers and schema inference.

```
val sales = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/daily_sales.csv")
```

### 2. Write CSV

Write the sales DataFrame in CSV format.

```
sales.write
  .mode("overwrite")
  .option("header", "true")
  .csv("output/csv")
```

### 3. Write JSON

Write the DataFrame in JSON format.

```
sales.write
  .mode("overwrite")
  .json("output/json")
```

### 4. Write Parquet

Write the DataFrame in Parquet format.

```
sales.write
  .mode("overwrite")
  .parquet("output/parquet")
```

### 5. Repartition Before Writing

Use `repartition(3, col("region"))` to redistribute records into three shuffle partitions based on region.

```
val repartitionedSales = sales.repartition(3, col("region"))
```

### 6. Partition Output by Date

Write Parquet data partitioned by year, month, and day.

```
repartitionedSales.write
  .mode("overwrite")
  .partitionBy("year", "month", "day")
  .parquet("output/partitioned_sales")
```

## Expected Output

### Source Data

```
+--------+--------+------+----------+--------+------+----+-----+---+
|sale_id |product |region|sale_date |quantity|amount|year|month|day|
+--------+--------+------+----------+--------+------+----+-----+---+
|S001    |Laptop  |South |2026-09-01|2       |120000|2026|9    |1  |
|S002    |Mouse   |West  |2026-09-01|5       |2500  |2026|9    |1  |
|S003    |Keyboard|North |2026-09-02|3       |4500  |2026|9    |2  |
|S004    |Monitor |South |2026-09-02|2       |30000 |2026|9    |2  |
|S005    |Laptop  |East  |2026-10-01|1       |60000 |2026|10   |1  |
|S006    |Mouse   |South |2026-10-01|10      |5000  |2026|10   |1  |
|S007    |Keyboard|West  |2026-10-02|4       |6000  |2026|10   |2  |
|S008    |Monitor |North |2026-10-02|1       |15000 |2026|10   |2  |
|S009    |Laptop  |South |2027-01-01|3       |180000|2027|1    |1  |
|S010    |Mouse   |East  |2027-01-01|8       |4000  |2027|1    |1  |
|S011    |Keyboard|South |2027-01-02|2       |3000  |2027|1    |2  |
|S012    |Monitor |West  |2027-01-02|3       |45000 |2027|1    |2  |
+--------+--------+------+----------+--------+------+----+-----+---+
```

### Record Counts

```
CSV records: 12
JSON records: 12
Parquet records: 12
```

### Partitioned Output Layout

```
output/partitioned_sales/
├── year=2026/
│   ├── month=9/
│   │   ├── day=1/
│   │   └── day=2/
│   └── month=10/
│       ├── day=1/
│       └── day=2/
└── year=2027/
    └── month=1/
        ├── day=1/
        └── day=2/
```

Each date directory contains Parquet part files. The exact number of files depends on the number of Spark partitions that contain records for each date.

## File Layout and Number of Output Files

- Spark writes output as a directory containing part files.
- Each task partition can produce an output file.
- `repartition()` performs a shuffle and changes the number and distribution of partitions.
- `partitionBy()` creates directory structures based on the selected columns.
- Repartitioning does not guarantee a fixed number of files in every date directory.

## How to Run

```
cd ~/spark-30-days/spark-day20
sbt run
```
