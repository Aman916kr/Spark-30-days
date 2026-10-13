import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HotelCatalogAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day21-Spark-Catalog")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Read hotel bookings
    val bookings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .csv("data/hotel_bookings.csv")

    println("\n========== SOURCE DATA ==========")
    bookings.show(false)

    // Create a temporary view
    bookings.createOrReplaceTempView("hotel_bookings")

    println("\n========== TEMPORARY VIEW QUERY ==========")
    spark.sql("""
      SELECT hotel_name, COUNT(*) AS total_bookings,
             SUM(amount) AS total_revenue
      FROM hotel_bookings
      GROUP BY hotel_name
      ORDER BY hotel_name
    """).show(false)

    // Create a database
    spark.sql("CREATE DATABASE IF NOT EXISTS hotel_analytics")
    spark.sql("USE hotel_analytics")

    // Register a persistent table
    bookings.write
      .mode("overwrite")
      .saveAsTable("bookings")

    println("\n========== DATABASES ==========")
    spark.sql("SHOW DATABASES").show(false)

    println("\n========== TABLES ==========")
    spark.sql("SHOW TABLES").show(false)

    println("\n========== TABLE SCHEMA ==========")
    spark.sql("DESCRIBE bookings").show(false)

    println("\n========== TABLE METADATA ==========")
    spark.sql("DESCRIBE EXTENDED bookings").show(50, false)

    println("\n========== QUERY REGISTERED TABLE ==========")
    spark.sql("""
      SELECT city, COUNT(*) AS total_bookings,
             SUM(amount) AS total_revenue
      FROM bookings
      GROUP BY city
      ORDER BY city
    """).show(false)

    println("\n========== CATALOG TABLE DETAILS ==========")
    println(spark.catalog.listTables("hotel_analytics").show(false))

    spark.stop()
  }
}
