import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustomerAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day13-Spark-SQL-Basics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Read CSV data
    val csvDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    println("\n========== CSV DATA ==========")
    csvDF.show()

    // 2. Read JSON data
    val jsonDF = spark.read
      .option("inferSchema", "true")
      .json("data/customers.json")

    println("\n========== JSON DATA ==========")
    jsonDF.show()

    // 3. Combine both DataFrames
    val customersDF = csvDF.unionByName(jsonDF)

    println("\n========== COMBINED CUSTOMER DATA ==========")
    customersDF.show()

    // 4. Inspect schema
    println("\n========== SCHEMA ==========")
    customersDF.printSchema()

    // 5. Select columns
    println("\n========== SELECT COLUMNS ==========")
    customersDF
      .select("customer_id", "name", "city", "total_spent")
      .show()

    // 6. Filter customers
    println("\n========== FILTER CUSTOMERS ==========")
    customersDF
      .filter(col("total_spent") > 15000)
      .select("name", "city", "total_spent")
      .show()

    // 7. Add calculated columns
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

    println("\n========== CUSTOMER ANALYTICS ==========")
    enrichedDF.show()

    // 8. Create temporary view
    enrichedDF.createOrReplaceTempView("customers")

    // 9. SQL: Customer count by city
    println("\n========== CUSTOMER COUNT BY CITY ==========")
    spark.sql(
      """
        SELECT city, COUNT(*) AS customer_count
        FROM customers
        GROUP BY city
        ORDER BY city
      """
    ).show()

    // 10. SQL: Revenue by customer segment
    println("\n========== REVENUE BY SEGMENT ==========")
    spark.sql(
      """
        SELECT
          customer_segment,
          COUNT(*) AS customer_count,
          SUM(total_spent) AS total_revenue
        FROM customers
        GROUP BY customer_segment
        ORDER BY total_revenue DESC
      """
    ).show()

    // 11. SQL: Top 3 customers
    println("\n========== TOP 3 CUSTOMERS ==========")
    spark.sql(
      """
        SELECT name, city, total_spent, customer_segment
        FROM customers
        ORDER BY total_spent DESC
        LIMIT 3
      """
    ).show()

    spark.stop()
  }
}
