import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Employee(
  employee_id: Int,
  name: String,
  department: String,
  role: String,
  salary: Double,
  bonus: Double
)

object EmployeePayroll {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day14-DataFrame-Dataset")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // 1. Read CSV into DataFrame
    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n========== EMPLOYEE DATAFRAME ==========")
    employeeDF.show()

    println("\n========== DATAFRAME SCHEMA ==========")
    employeeDF.printSchema()

    // 2. Convert DataFrame to typed Dataset
    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

    println("\n========== TYPED EMPLOYEE DATASET ==========")
    employeeDS.show()

    // 3. Typed Dataset operations
    val payrollDS = employeeDS.map { employee =>
      val totalSalary = employee.salary + employee.bonus

      employee.copy(
        salary = totalSalary
      )
    }

    println("\n========== DATASET PAYROLL ==========")
    payrollDS.show()

    // 4. Convert Dataset back to DataFrame
    val payrollDF = payrollDS.toDF()

    println("\n========== DATASET TO DATAFRAME ==========")
    payrollDF.show()

    // 5. Department-wise payroll using DataFrame
    println("\n========== DEPARTMENT PAYROLL ==========")
    payrollDF
      .groupBy("department")
      .agg(
        sum("salary").alias("total_payroll"),
        count("*").alias("employee_count")
      )
      .orderBy("department")
      .show()

    // 6. Typed Dataset filtering
    println("\n========== EMPLOYEES WITH PAYROLL ABOVE 75000 ==========")
    payrollDS
      .filter(employee => employee.salary > 75000)
      .show()

    // 7. RDD, DataFrame and Dataset comparison
    println("\n========== RDD, DATAFRAME AND DATASET ==========")
    println("RDD: Low-level distributed collection of objects.")
    println("DataFrame: Distributed data organized into named columns.")
    println("Dataset: Typed distributed collection with a known schema.")

    println("\nType safety:")
    println("Dataset provides compile-time type checking for typed operations.")

    println("\nCatalyst optimization:")
    println("Spark SQL optimizes DataFrame and Dataset query plans.")

    spark.stop()
  }
}
