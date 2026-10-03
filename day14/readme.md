# Day14DataFrameDataset

A beginner-friendly Spark project that demonstrates DataFrame and Dataset operations by building a typed employee payroll pipeline.

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.9
- SBT 1.11.7
- Ubuntu Linux (WSL)
- JDK 11

## DataFrame and Dataset Concepts Covered

- Case class
- DataFrame
- Dataset
- DataFrame to Dataset conversion
- Dataset to DataFrame conversion
- Typed Dataset operations
- `map`
- `filter`
- `groupBy`
- `agg`
- `sum`
- RDD vs DataFrame vs Dataset
- Type safety
- Catalyst optimization

## Project Structure

    spark-day14/
    ├── build.sbt
    ├── data/
    │   └── employees.csv
    └── src/
        └── main/
            └── scala/
                └── EmployeePayroll.scala

## Practice Tasks

### 1. Create an Employee Case Class

Define a case class to represent employee records with a known structure.

    case class Employee(
      employee_id: Int,
      name: String,
      department: String,
      role: String,
      salary: Double,
      bonus: Double
    )

### 2. Read CSV into a DataFrame

Read employee records from the CSV file.

    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

### 3. Convert DataFrame to Dataset

Use `.as[Employee]` to convert the DataFrame into a typed Dataset.

    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

The case class provides a defined type for each employee record.

### 4. Perform Typed Dataset Operations

Calculate total payroll for each employee by adding the bonus to the salary.

    val payrollDS = employeeDS.map { employee =>
      val totalSalary = employee.salary + employee.bonus

      employee.copy(
        salary = totalSalary
      )
    }

### 5. Convert Dataset Back to DataFrame

Use `toDF()` to convert the typed Dataset back into a DataFrame.

    val payrollDF = payrollDS.toDF()

### 6. Calculate Department-wise Payroll

Group employees by department and calculate total payroll and employee count.

    payrollDF
      .groupBy("department")
      .agg(
        sum("salary").alias("total_payroll"),
        count("*").alias("employee_count")
      )
      .show()

### 7. Filter Using Dataset

Find employees whose calculated payroll is greater than 75,000.

    payrollDS
      .filter(employee => employee.salary > 75000)
      .show()

### 8. Compare RDD, DataFrame and Dataset

| Feature | RDD | DataFrame | Dataset |
|---|---|---|---|
| Structure | Distributed collection | Rows and named columns | Typed objects |
| Type safety | Compile-time for typed RDDs | Not strongly typed | Compile-time for typed operations |
| Optimization | Less SQL-level optimization | Catalyst optimizer | Catalyst optimizer |
| API level | Low-level | High-level | High-level and typed |

### 9. Understand Type Safety

Dataset operations use the case class type, allowing Scala to check typed operations at compile time.

Example:

    employeeDS.filter(employee => employee.salary > 75000)

The `employee` variable is an `Employee` object.

### 10. Understand Catalyst Optimization

Catalyst is Spark SQL's optimizer. It optimizes query plans for DataFrame and Dataset operations before execution.

## How to Run

Navigate to the project directory:

    cd ~/spark-30-days/spark-day14

Run the project:

    sbt run

## Output

The project reads employee records, converts the DataFrame into a typed Dataset, calculates payroll, converts the Dataset back to a DataFrame, and performs department-wise aggregation.

### Employee Payroll

Expected calculated payroll values:

    Alice    85000
    Bob      65000
    Charlie  73000
    David    61000
    Emma    105000
    Frank    80000

### Department-wise Payroll

Expected results:

    +-----------+-------------+--------------+
    | department|total_payroll|employee_count|
    +-----------+-------------+--------------+
    |Engineering|       255000|             3|
    |    Finance|       141000|             2|
    |         HR|        73000|             1|
    +-----------+-------------+--------------+

### Employees with Payroll Above 75000

Expected employees:

    Alice  85000
    Emma  105000
    Frank  80000
