import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BroadcastJoinAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19-Broadcast-Join")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Large fact DataFrame: transactions
    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")
      .alias("t")

    // Small reference DataFrame: branch master
    val branches = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/branch_master.csv")
      .alias("b")

    println("\n========== TRANSACTIONS ==========")
    transactions.show(false)

    println("\n========== BRANCH MASTER ==========")
    branches.show(false)

    // Broadcast join
    println("\n========== BROADCAST JOIN ==========")

    val broadcastResult = transactions.join(
      broadcast(branches),
      col("t.branch_id") === col("b.branch_id"),
      "inner"
    )

    broadcastResult.select(
      col("t.transaction_id"),
      col("t.branch_id"),
      col("b.branch_name"),
      col("b.city"),
      col("b.region"),
      col("t.customer_id"),
      col("t.amount")
    ).show(false)

    println("\n========== REVENUE BY REGION ==========")

    broadcastResult.groupBy(col("b.region"))
      .agg(
        count("*").alias("total_transactions"),
        sum(col("t.amount")).alias("total_revenue")
      )
      .orderBy(col("b.region"))
      .show(false)

    println("\n========== BROADCAST JOIN EXECUTION PLAN ==========")
    broadcastResult.explain(true)

    // Compare with a regular join
    println("\n========== REGULAR JOIN EXECUTION PLAN ==========")

    val regularResult = transactions.join(
      branches,
      col("t.branch_id") === col("b.branch_id"),
      "inner"
    )

    regularResult.explain(true)

    spark.stop()
  }
}
