# Day13SparkSQLBasics

A beginner-friendly Spark project that demonstrates Spark SQL and DataFrame operations by building a customer analytics report using CSV and JSON data.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.9
- SBT 1.11.7
- Ubuntu Linux (WSL)
- JDK 11

## Spark SQL Concepts Covered

- SparkSession
- DataFrame
- Reading CSV and JSON files
- `unionByName`
- `printSchema`
- `select`
- `filter`
- `withColumn`
- `when` and `otherwise`
- Spark SQL expressions
- Temporary views
- SQL queries using `spark.sql()`
- `GROUP BY`, `ORDER BY`, `COUNT`, `SUM`, and `LIMIT`

## Project Structure

    Day13SparkSQLBasics/
    ├── build.sbt
    ├── data/
    │   ├── customers.csv
    │   └── customers.json
    └── src/
        └── main/
            └── scala/
                └── CustomerAnalytics.scala

## Practice Tasks

### 1. Read CSV Data

Read customer information from a CSV file using Spark.

    val csvDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

### 2. Read JSON Data

Read customer information from a JSON file.

    val jsonDF = spark.read
      .option("inferSchema", "true")
      .json("data/customers.json")

### 3. Combine DataFrames

Combine CSV and JSON customer records using `unionByName`.

    val customersDF = csvDF.unionByName(jsonDF)

The combined DataFrame contains 8 customers.

### 4. Inspect the Schema

Use `printSchema()` to inspect column names and data types.

    customersDF.printSchema()

### 5. Select Columns

Select specific columns from the customer DataFrame.

    customersDF.select(
      "customer_id",
      "name",
      "city",
      "total_spent"
    ).show()

### 6. Filter Customers

Find customers whose total spending is greater than 15,000.

    customersDF
      .filter(col("total_spent") > 15000)
      .select("name", "city", "total_spent")
      .show()

### 7. Add Columns Using Expressions

Use `withColumn()` and conditional expressions to create customer segments.

    val enrichedDF = customersDF
      .withColumn(
        "customer_segment",
        when(col("total_spent") >= 20000, "Premium")
          .when(col("total_spent") >= 10000, "Regular")
          .otherwise("Basic")
      )
      .withColumn(
        "average_order_value",
        round(col("total_spent") / col("orders"), 2)
      )

Customer segments:
- Premium: Spending greater than or equal to 20,000
- Regular: Spending greater than or equal to 10,000
- Basic: Spending below 10,000

### 8. Register a Temporary View

Register the enriched DataFrame as a temporary SQL view.

    enrichedDF.createOrReplaceTempView("customers")

### 9. Customer Count by City

Use Spark SQL to count customers in each city.

    spark.sql("""
      SELECT city, COUNT(*) AS customer_count
      FROM customers
      GROUP BY city
      ORDER BY city
    """).show()

### 10. Revenue by Customer Segment

Calculate customer count and total revenue for each segment.

    spark.sql("""
      SELECT
        customer_segment,
        COUNT(*) AS customer_count,
        SUM(total_spent) AS total_revenue
      FROM customers
      GROUP BY customer_segment
      ORDER BY total_revenue DESC
    """).show()

### 11. Top 3 Customers

Find the three customers with the highest spending.

    spark.sql("""
      SELECT name, city, total_spent, customer_segment
      FROM customers
      ORDER BY total_spent DESC
      LIMIT 3
    """).show()

## How to Run

Navigate to the project directory:

    cd ~/spark-30-days/spark-day13

Run the project:

    sbt run

## Output

The project successfully read CSV and JSON data, combined both DataFrames, inspected the schema, performed transformations, and executed SQL queries.

### Combined Customer Data

    Total customers: 8

### Customer Count by City

    +---------+--------------+
    |     city|customer_count|
    +---------+--------------+
    |Bangalore|             3|
    |  Chennai|             2|
    |Hyderabad|             3|
    +---------+--------------+

### Revenue by Customer Segment

    +----------------+--------------+-------------+
    |customer_segment|customer_count|total_revenue|
    +----------------+--------------+-------------+
    |         Premium|             3|        79000|
    |         Regular|             3|        45000|
    |           Basic|             2|        13000|
    +----------------+--------------+-------------+

### Top 3 Customers

    +-----+---------+-----------+----------------+
    | name|     city|total_spent|customer_segment|
    +-----+---------+-----------+----------------+
    | Emma|Bangalore|      32000|         Premium|
    |  Bob|Bangalore|      25000|         Premium|
    |Grace|Hyderabad|      22000|         Premium|
    +-----+---------+-----------+----------------+
