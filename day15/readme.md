# Day15UDFPractice

A beginner-friendly Spark project that demonstrates Scala UDFs and built-in Spark functions by creating a customer transaction risk analysis pipeline.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.9
- SBT 1.11.7
- Ubuntu Linux (WSL)
- JDK 11

## UDF Concepts Covered

- User Defined Function (UDF)
- Scala UDF
- `withColumn`
- `when` and `otherwise`
- Built-in Spark SQL functions
- UDF registration
- Spark SQL temporary views
- Customer risk classification
- UDF vs built-in functions

## Project Structure

    spark-day15/
    ├── build.sbt
    ├── data/
    │   └── transactions.csv
    └── src/
        └── main/
            └── scala/
                └── CustomerRiskAnalysis.scala

## Practice Tasks

### 1. Read Transaction Data

Read customer transaction records from a CSV file.

    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

### 2. Create a Scala UDF

Create a custom function to classify transaction amounts into risk categories.

    val classifyRisk = udf((amount: Double) => {
      if (amount < 10000) "Low"
      else if (amount < 50000) "Medium"
      else if (amount < 100000) "High"
      else "Critical"
    })

### 3. Apply UDF Using withColumn

Add a new column named `risk_category` using the UDF.

    val udfResult = transactionsDF.withColumn(
      "risk_category",
      classifyRisk(col("transaction_amount").cast("double"))
    )

### 4. Compare with Built-in Spark Functions

Use Spark's built-in conditional expressions to perform the same classification.

    val builtInResult = transactionsDF.withColumn(
      "risk_category",
      when(col("transaction_amount") < 10000, "Low")
        .when(col("transaction_amount") < 50000, "Medium")
        .when(col("transaction_amount") < 100000, "High")
        .otherwise("Critical")
    )

### 5. Register UDF with Spark SQL

Register the Scala function so it can be called from SQL.

    spark.udf.register(
      "classify_risk",
      (amount: Double) => {
        if (amount < 10000) "Low"
        else if (amount < 50000) "Medium"
        else if (amount < 100000) "High"
        else "Critical"
      }
    )

### 6. Use Registered UDF in SQL

Create a temporary view and call the registered UDF from a SQL query.

    udfResult.createOrReplaceTempView("transactions")

    spark.sql("""
      SELECT
        transaction_id,
        customer_id,
        transaction_amount,
        classify_risk(CAST(transaction_amount AS DOUBLE)) AS risk_category
      FROM transactions
      ORDER BY transaction_id
    """).show()

### 7. Build Customer Risk Report

Group transactions by customer and risk category to calculate total transaction value and transaction count.

    udfResult
      .groupBy("customer_id", "risk_category")
      .agg(
        sum("transaction_amount").alias("total_transaction_value"),
        count("*").alias("transaction_count")
      )
      .show()

### 8. UDF vs Built-in Functions

| Feature | UDF | Built-in Spark Function |
|---|---|---|
| Custom logic | Supports custom Scala logic | Uses predefined Spark operations |
| Catalyst optimization | Less optimization visibility | Catalyst can optimize expressions |
| Usage | Useful for specialized logic | Preferred for standard transformations |
| Example | `classifyRisk()` | `when()` and `otherwise()` |

## Risk Classification

| Transaction Amount | Risk Category |
|---|---|
| Below 10,000 | Low |
| 10,000–49,999 | Medium |
| 50,000–99,999 | High |
| 100,000 and above | Critical |

## How to Run

Navigate to the project directory:

    cd ~/spark-30-days/spark-day15

Run the project:

    sbt run

## Output

The project reads transaction records, applies risk classification using a Scala UDF and built-in Spark expressions, registers the UDF for SQL use, and generates a customer risk report.

### Risk Classification

Expected results:

    T001  C101  500      Low
    T002  C102  15000    Medium
    T003  C103  45000    Medium
    T004  C104  75000    High
    T005  C105  120000   Critical
    T006  C101  2500     Low
    T007  C102  30000    Medium
    T008  C103  90000    High
    T009  C104  8000     Low
    T010  C105  200000   Critical

### Customer Risk Report

Expected grouped results:

    C101  Low       3000    2
    C102  Medium    45000   2
    C103  High      90000   1
    C103  Medium    45000   1
    C104  High      75000   1
    C104  Low       8000    1
    C105  Critical  320000  2
