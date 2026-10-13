import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object EcommercePipeline {

  case class CleanOrder(
    orderId: String,
    customerId: String,
    productId: String,
    quantity: Int,
    unitPrice: Double,
    orderDate: String,
    status: String
  )

  def main(args: Array[String]): Unit = {

    val basePath = args.headOption.getOrElse(".")

    val conf = new SparkConf()
      .setAppName("Day29-EcommercePipeline")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rawPath = s"$basePath/data"
    val outputPath = s"$basePath/output"

    // 1. READ RAW DATA
    val rawOrders = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .csv(s"$rawPath/orders.csv")

    val customers = spark.read
      .option("header", "true")
      .csv(s"$rawPath/customers.csv")

    val products = spark.read
      .option("header", "true")
      .csv(s"$rawPath/products.csv")

    println("===== RAW ORDERS =====")
    rawOrders.show(false)

    // Preserve raw data for auditing and reprocessing
    rawOrders.write
      .mode("overwrite")
      .parquet(s"$outputPath/raw")

    // 2. CLEAN AND VALIDATE USING AN RDD
    // Keep completed orders with valid required fields and values.
    val cleanOrderRDD = rawOrders.rdd.flatMap { row =>
      try {
        def value(name: String): String =
          Option(row.getAs[String](name))
            .getOrElse("")
            .trim

        val orderId = value("orderId")
        val customerId = value("customerId")
        val productId = value("productId")
        val quantity = value("quantity").toInt
        val unitPrice = value("unitPrice").toDouble
        val orderDate = value("orderDate")
        val status = value("status").toUpperCase

        val valid =
          orderId.nonEmpty &&
          customerId.nonEmpty &&
          productId.nonEmpty &&
          quantity > 0 &&
          unitPrice >= 0 &&
          orderDate.matches("\\d{4}-\\d{2}-\\d{2}") &&
          status == "COMPLETED"

        if (valid) {
          Some(
            CleanOrder(
              orderId,
              customerId,
              productId,
              quantity,
              unitPrice,
              orderDate,
              status
            )
          )
        } else {
          None
        }
      } catch {
        case _: Exception => None
      }
    }

    val cleanOrders = spark.createDataset(cleanOrderRDD)
      .toDF()
      .withColumn(
        "totalAmount",
        col("quantity") * col("unitPrice")
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Materialize persisted data
    println(s"Valid completed orders: ${cleanOrders.count()}")

    println("===== CLEAN ORDERS =====")
    cleanOrders.show(false)

    cleanOrders.write
      .mode("overwrite")
      .parquet(s"$outputPath/clean")

    // 3. PAIR RDD AGGREGATION
    // Calculate total spending per customer using key-value pairs.
    val customerSpendRDD = cleanOrders
      .select("customerId", "totalAmount")
      .rdd
      .map(row => (row.getString(0), row.getDouble(1)))
      .reduceByKey(_ + _)

    val customerSpendDF = customerSpendRDD
      .map {
        case (customerId, totalSpend) =>
          (customerId, totalSpend)
      }
      .toDF("customerId", "customerTotalSpend")

    println("===== CUSTOMER SPEND: PAIR RDD =====")
    customerSpendDF.orderBy("customerId").show(false)

    // 4. BROADCAST SMALL REFERENCE DATA
    // Suitable for small customer/product reference tables.
    val broadcastCustomers = broadcast(customers)
    val broadcastProducts = broadcast(products)

    // 5. ENRICH USING DATAFRAME JOINS
    // Inner joins intentionally exclude orders with missing references.
    val enriched = cleanOrders
      .join(
        broadcastCustomers,
        Seq("customerId"),
        "inner"
      )
      .join(
        broadcastProducts,
        Seq("productId"),
        "inner"
      )
      .withColumn(
        "orderDate",
        to_date(col("orderDate"), "yyyy-MM-dd")
      )
      .withColumn(
        "orderMonth",
        date_format(col("orderDate"), "yyyy-MM")
      )
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Enriched order count: ${enriched.count()}")

    println("===== ENRICHED ORDERS =====")
    enriched.show(false)

    enriched.write
      .mode("overwrite")
      .partitionBy("category")
      .parquet(s"$outputPath/enriched")

    // 6. WINDOW FUNCTIONS
    // Running total for each customer ordered chronologically.
    val customerWindow = Window
      .partitionBy("customerId")
      .orderBy(col("orderDate"), col("orderId"))
      .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
      )

    val rankedOrders = enriched
      .withColumn(
        "runningCustomerSpend",
        sum(col("totalAmount")).over(customerWindow)
      )
      .withColumn(
        "customerOrderNumber",
        row_number().over(
          Window
            .partitionBy("customerId")
            .orderBy(col("orderDate"), col("orderId"))
        )
      )

    println("===== CUSTOMER WINDOW ANALYSIS =====")
    rankedOrders.select(
      "orderId",
      "customerId",
      "orderDate",
      "totalAmount",
      "runningCustomerSpend",
      "customerOrderNumber"
    ).orderBy("customerId", "orderDate", "orderId").show(false)

    rankedOrders.write
      .mode("overwrite")
      .parquet(s"$outputPath/customer_windows")

    // 7. CATEGORY AND MONTHLY AGGREGATION
    val categorySales = enriched
      .groupBy("category", "orderMonth")
      .agg(
        countDistinct("orderId").alias("orderCount"),
        sum("quantity").alias("unitsSold"),
        sum("totalAmount").alias("totalRevenue"),
        avg("totalAmount").alias("averageOrderValue")
      )

    println("===== CATEGORY SALES REPORT =====")
    categorySales
      .orderBy("orderMonth", "category")
      .show(false)

    // Partition output by category for downstream reads.
    categorySales.write
      .mode("overwrite")
      .partitionBy("category")
      .parquet(s"$outputPath/category_sales")

    // 8. REVENUE BY REGION
    val regionalSales = enriched
      .groupBy("region")
      .agg(
        countDistinct("orderId").alias("orderCount"),
        sum("totalAmount").alias("totalRevenue")
      )
      .orderBy(desc("totalRevenue"))

    println("===== REGIONAL SALES REPORT =====")
    regionalSales.show(false)

    regionalSales.write
      .mode("overwrite")
      .parquet(s"$outputPath/regional_sales")

    // 9. SUMMARY
    println("===== PIPELINE SUMMARY =====")
    println(s"Raw orders: ${rawOrders.count()}")
    println(s"Clean completed orders: ${cleanOrders.count()}")
    println(s"Enriched orders: ${enriched.count()}")
    println(s"Output directory: $outputPath")

    // 10. RELEASE CACHED DATA
    enriched.unpersist()
    cleanOrders.unpersist()

    spark.stop()
  }
}
