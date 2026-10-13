import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object EcommerceCapstone {

  case class Order(
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
      .setAppName("Day30-EcommerceCapstone")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val invalidRows = spark.sparkContext.longAccumulator(
      "Invalid order records"
    )

    // ==================================================
    // PART A: BATCH PIPELINE
    // ==================================================

    println("\n========== BATCH PIPELINE ==========")

    val rawOrders = spark.read
      .option("header", "true")
      .csv(s"$basePath/data/orders.csv")

    val customers = spark.read
      .option("header", "true")
      .csv(s"$basePath/data/customers.csv")

    val products = spark.read
      .option("header", "true")
      .csv(s"$basePath/data/products.csv")

    // Preserve the raw source for auditing
    rawOrders.write
      .mode("overwrite")
      .parquet(s"$basePath/output/raw_orders")

    // 1. RDD: parse and validate orders
    val parsedOrders = rawOrders.rdd.flatMap { row =>
      try {
        def get(name: String): String =
          Option(row.getAs[String](name))
            .getOrElse("")
            .trim

        val orderId = get("orderId")
        val customerId = get("customerId")
        val productId = get("productId")
        val quantity = get("quantity").toInt
        val unitPrice = get("unitPrice").toDouble
        val orderDate = get("orderDate")
        val status = get("status").toUpperCase

        val valid =
          orderId.nonEmpty &&
          customerId.nonEmpty &&
          productId.nonEmpty &&
          quantity > 0 &&
          unitPrice >= 0 &&
          orderDate.matches("\\d{4}-\\d{2}-\\d{2}") &&
          Set("COMPLETED", "CANCELLED").contains(status)

        if (!valid) {
          invalidRows.add(1L)
          None
        } else {
          Some(
            Order(
              orderId,
              customerId,
              productId,
              quantity,
              unitPrice,
              orderDate,
              status
            )
          )
        }
      } catch {
        case _: Exception =>
          invalidRows.add(1L)
          None
      }
    }

    val cleanOrders = spark.createDataset(parsedOrders)
      .toDF()
      .filter($"status" === "COMPLETED")
      .withColumn("orderDate", to_date($"orderDate", "yyyy-MM-dd"))
      .withColumn("totalAmount", $"quantity" * $"unitPrice")
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Materialize cache and accumulator before reading their values
    val cleanCount = cleanOrders.count()

    println(s"Clean completed orders: $cleanCount")
    println(s"Invalid records rejected: ${invalidRows.value}")

    cleanOrders.write
      .mode("overwrite")
      .parquet(s"$basePath/output/clean_orders")

    // 2. Pair RDD: customer-level spending
    val customerSpendRDD = cleanOrders
      .select("customerId", "totalAmount")
      .rdd
      .map(row => (row.getString(0), row.getDouble(1)))
      .reduceByKey(_ + _)

    val customerSpend = customerSpendRDD.toDF(
      "customerId",
      "totalSpend"
    )

    println("\n========== CUSTOMER SPEND: PAIR RDD ==========")
    customerSpend.orderBy("customerId").show(false)

    // 3. Broadcast reference data and enrich orders
    val enriched = cleanOrders
      .join(broadcast(customers), Seq("customerId"), "inner")
      .join(broadcast(products), Seq("productId"), "inner")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val enrichedCount = enriched.count()

    println(s"Enriched completed orders: $enrichedCount")

    // 4. UDF: classify order value
    // Built-in expressions are preferred when a UDF isn't needed.
    val valueBand = udf { amount: Double =>
      if (amount >= 2000) "HIGH"
      else if (amount >= 1000) "MEDIUM"
      else "LOW"
    }

    val classified = enriched
      .withColumn("valueBand", valueBand($"totalAmount"))

    // 5. SQL window: running spend by customer
    val customerWindow = Window
      .partitionBy("customerId")
      .orderBy($"orderDate", $"orderId")
      .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
      )

    val windowReport = classified
      .withColumn(
        "runningSpend",
        sum($"totalAmount").over(customerWindow)
      )
      .withColumn(
        "orderRank",
        row_number().over(
          Window
            .partitionBy("customerId")
            .orderBy($"orderDate", $"orderId")
        )
      )

    println("\n========== WINDOW REPORT ==========")
    windowReport.select(
      "orderId",
      "customerId",
      "customerName",
      "productName",
      "totalAmount",
      "valueBand",
      "runningSpend",
      "orderRank"
    ).show(false)

    // 6. Partition tuning demonstration
    val partitioned = windowReport.repartition(4, $"category")

    println(
      s"Partitions after repartition: ${partitioned.rdd.getNumPartitions}"
    )

    // Inspect physical plan: look for Exchange and join strategies
    println("\n========== PHYSICAL PLAN ==========")
    partitioned.explain(true)

    // 7. Aggregate reports
    val categoryReport = partitioned
      .groupBy("category")
      .agg(
        countDistinct("orderId").alias("orderCount"),
        sum("quantity").alias("unitsSold"),
        sum("totalAmount").alias("revenue")
      )

    println("\n========== CATEGORY REPORT ==========")
    categoryReport.orderBy(desc("revenue")).show(false)

    categoryReport.write
      .mode("overwrite")
      .partitionBy("category")
      .parquet(s"$basePath/output/category_report")

    windowReport.write
      .mode("overwrite")
      .parquet(s"$basePath/output/customer_windows")

    // 8. Release cached data after its last batch use
    enriched.unpersist()
    cleanOrders.unpersist()

    // ==================================================
    // PART B: REAL-TIME STREAMING PIPELINE
    // ==================================================

    println("\n========== STARTING STREAMING PIPELINE ==========")
    println("Expected event format:")
    println("eventId,customerId,productId,quantity,unitPrice,eventTime")

    val streamInput = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val splitEvent = split($"value", ",", -1)

    val events = streamInput
      .select(
        splitEvent.getItem(0).as("eventId"),
        splitEvent.getItem(1).as("customerId"),
        splitEvent.getItem(2).as("productId"),
        splitEvent.getItem(3).cast("int").as("quantity"),
        splitEvent.getItem(4).cast("double").as("unitPrice"),
        to_timestamp(
          splitEvent.getItem(5),
          "yyyy-MM-dd'T'HH:mm:ss"
        ).as("eventTime")
      )
      .filter(
        $"eventId".isNotNull &&
        $"customerId".isNotNull &&
        $"productId".isNotNull &&
        $"quantity" > 0 &&
        $"unitPrice" >= 0 &&
        $"eventTime".isNotNull
      )

    val streamingReport = events
      .withWatermark("eventTime", "2 minutes")
      .groupBy(
        window($"eventTime", "1 minute"),
        $"productId"
      )
      .agg(
        sum($"quantity").alias("unitsSold"),
        sum($"quantity" * $"unitPrice").alias("windowRevenue")
      )

    val query = streamingReport.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option(
        "checkpointLocation",
        s"$basePath/checkpoints/streaming"
      )
      .start()

    query.awaitTermination()
  }
}
