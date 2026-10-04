import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HospitalRevenueAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Hospital Revenue Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val hospitalDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hospital_revenue.csv")

    println("=== Hospital Data ===")
    hospitalDF.show()

    println("=== Overall Revenue Statistics ===")
    hospitalDF.agg(
      count("patient_id").as("total_patients"),
      sum("revenue").as("total_revenue"),
      avg("revenue").as("average_revenue"),
      min("revenue").as("minimum_revenue"),
      max("revenue").as("maximum_revenue")
    ).show()

    println("=== Department-wise Revenue Statistics ===")
    val departmentStats = hospitalDF
      .groupBy("department")
      .agg(
        count("patient_id").as("patient_count"),
        sum("revenue").as("total_revenue"),
        avg("revenue").as("average_revenue"),
        min("revenue").as("minimum_revenue"),
        max("revenue").as("maximum_revenue")
      )

    departmentStats.orderBy(desc("total_revenue")).show()

    println("=== Department and Doctor-wise Revenue ===")
    hospitalDF
      .groupBy("department", "doctor")
      .agg(
        count("patient_id").as("patient_count"),
        sum("revenue").as("total_revenue")
      )
      .orderBy("department", "doctor")
      .show()

    println("=== Departments with Total Revenue Above 50000 ===")
    departmentStats
      .filter(col("total_revenue") > 50000)
      .orderBy(desc("total_revenue"))
      .show()

    spark.stop()
  }
}
