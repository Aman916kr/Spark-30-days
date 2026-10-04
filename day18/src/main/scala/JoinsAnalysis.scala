import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18-Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")
      .alias("o")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")
      .alias("c")

    val payments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/payments.csv")
      .alias("p")

    println("\n========== INNER JOIN: ORDERS + CUSTOMERS ==========")

    val innerJoin = orders.join(
      customers,
      col("o.customer_id") === col("c.customer_id"),
      "inner"
    )

    innerJoin.select(
      col("o.order_id"),
      col("o.customer_id"),
      col("c.customer_name"),
      col("c.city"),
      col("o.amount")
    ).show(false)

    println("\n========== LEFT JOIN: ORDERS + PAYMENTS ==========")

    val leftJoin = orders.join(
      payments,
      col("o.order_id") === col("p.order_id"),
      "left"
    )

    leftJoin.select(
      col("o.order_id"),
      col("o.amount"),
      col("p.payment_method"),
      col("p.payment_status"),
      col("p.paid_amount")
    ).withColumn(
      "payment_status",
      coalesce(col("payment_status"), lit("Not Paid"))
    ).show(false)

    println("\n========== RIGHT JOIN: ORDERS + CUSTOMERS ==========")

    orders.join(
      customers,
      col("o.customer_id") === col("c.customer_id"),
      "right"
    ).select(
      col("o.order_id"),
      col("c.customer_id"),
      col("c.customer_name"),
      col("c.city")
    ).show(false)

    println("\n========== FULL OUTER JOIN: ORDERS + CUSTOMERS ==========")

    orders.join(
      customers,
      col("o.customer_id") === col("c.customer_id"),
      "full"
    ).select(
      col("o.order_id"),
      coalesce(col("o.customer_id"), col("c.customer_id")).alias("customer_id"),
      col("c.customer_name"),
      col("c.city"),
      col("o.amount")
    ).show(false)

    println("\n========== THREE-TABLE JOIN ==========")

    val orderDetails = orders
      .join(customers, col("o.customer_id") === col("c.customer_id"), "left")
      .join(payments, col("o.order_id") === col("p.order_id"), "left")

    orderDetails.select(
      col("o.order_id"),
      col("c.customer_name"),
      col("c.city"),
      col("o.amount"),
      coalesce(col("p.payment_status"), lit("Not Paid")).alias("payment_status"),
      col("p.payment_method"),
      col("p.paid_amount")
    ).show(false)

    println("\n========== SHUFFLE SORT MERGE JOIN PLAN ==========")

    orders.join(
      customers,
      col("o.customer_id") === col("c.customer_id"),
      "inner"
    ).explain(true)

    spark.stop()
  }
}
