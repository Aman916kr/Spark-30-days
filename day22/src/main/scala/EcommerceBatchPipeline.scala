import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EcommerceBatchPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day22-Ecommerce-Batch-Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Read raw transactions
    val rawTransactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    val customers = spark.read
      .option("header", "true")
      .csv("data/customers.csv")

    val products = spark.read
      .option("header", "true")
      .csv("data/products.csv")

    println("\n========== RAW TRANSACTIONS ==========")
    rawTransactions.show(false)

    println(s"Raw transaction count: ${rawTransactions.count()}")

    // 2. Clean invalid records
    val cleanedTransactions = rawTransactions
      .withColumn("quantity", col("quantity").cast(IntegerType))
      .withColumn("unit_price", col("unit_price").cast(DoubleType))
      .withColumn(
        "transaction_date",
        to_date(col("transaction_date"), "yyyy-MM-dd")
      )
      .filter(
        col("transaction_id").isNotNull &&
        col("customer_id").isNotNull &&
        col("product_id").isNotNull &&
        col("transaction_date").isNotNull &&
        col("quantity") > 0 &&
        col("unit_price") > 0 &&
        upper(trim(col("status"))) === "COMPLETED"
      )
      .dropDuplicates("transaction_id")
      .withColumn(
        "revenue",
        col("quantity") * col("unit_price")
      )
      .withColumn("year", year(col("transaction_date")))
      .withColumn("month", month(col("transaction_date")))
      .withColumn("day", dayofmonth(col("transaction_date")))

    println("\n========== CLEANED TRANSACTIONS ==========")
    cleanedTransactions.show(false)

    println(s"Cleaned transaction count: ${cleanedTransactions.count()}")

    // 3. Join customer and product data
    val enrichedTransactions = cleanedTransactions
      .join(customers, Seq("customer_id"), "inner")
      .join(products, Seq("product_id"), "inner")

    println("\n========== ENRICHED TRANSACTIONS ==========")
    enrichedTransactions
      .select(
        "transaction_id",
        "customer_name",
        "city",
        "product_name",
        "category",
        "transaction_date",
        "quantity",
        "unit_price",
        "revenue"
      )
      .show(false)

    // 4. Daily sales aggregation
    val dailySales = enrichedTransactions
      .groupBy(
        col("transaction_date"),
        col("year"),
        col("month"),
        col("day"),
        col("city"),
        col("category")
      )
      .agg(
        count("transaction_id").alias("total_transactions"),
        sum("quantity").alias("total_quantity"),
        sum("revenue").alias("total_revenue")
      )

    println("\n========== DAILY SALES SUMMARY ==========")
    dailySales
      .orderBy("transaction_date", "city", "category")
      .show(false)

    // 5. Write partitioned Parquet output
    dailySales.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/daily_sales")

    println("\n========== OUTPUT VERIFICATION ==========")

    val savedSales = spark.read.parquet("output/daily_sales")

    println(s"Output summary row count: ${savedSales.count()}")

    savedSales
      .groupBy("year", "month", "day")
      .agg(sum("total_revenue").alias("daily_revenue"))
      .orderBy("year", "month", "day")
      .show(false)

    println("\nPartitioned Parquet output: output/daily_sales")

    spark.stop()
  }
}
