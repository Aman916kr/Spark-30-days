import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FileFormatsAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20-File-Formats")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Read CSV
    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .csv("data/daily_sales.csv")
      .withColumn("sale_date", to_date(col("sale_date")))
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    println("\n========== SOURCE DATA ==========")
    sales.show(false)

    // Write CSV
    sales.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/csv")

    // Write JSON
    sales.write
      .mode("overwrite")
      .json("output/json")

    // Write Parquet
    sales.write
      .mode("overwrite")
      .parquet("output/parquet")

    // Repartition before writing
    val repartitionedSales = sales.repartition(3, col("region"))

    repartitionedSales.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/partitioned_sales")

    println("\n========== OUTPUT FILE COUNTS ==========")

    println("CSV files:")
    println(spark.read.option("header", "true").csv("output/csv").count())

    println("JSON records:")
    println(spark.read.json("output/json").count())

    println("Parquet records:")
    println(spark.read.parquet("output/parquet").count())

    println("\n========== PARTITIONED DATA ==========")
    spark.read.parquet("output/partitioned_sales")
      .orderBy("year", "month", "day")
      .show(false)

    println("\n========== PARTITIONED OUTPUT FILE LAYOUT ==========")
    println("output/partitioned_sales/year=2026/month=9/day=1/ ...")
    println("output/partitioned_sales/year=2026/month=9/day=2/ ...")
    println("output/partitioned_sales/year=2026/month=10/day=1/ ...")
    println("output/partitioned_sales/year=2026/month=10/day=2/ ...")
    println("output/partitioned_sales/year=2027/month=1/day=1/ ...")
    println("output/partitioned_sales/year=2027/month=1/day=2/ ...")

    spark.stop()
  }
}
