import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustomerRiskAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day15-UDF-Practice")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Read transaction data
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    println("\n========== TRANSACTION DATA ==========")
    transactionsDF.show()

    // 2. Create Scala UDF for risk classification
    val classifyRisk = udf((amount: Double) => {
      if (amount < 10000) "Low"
      else if (amount < 50000) "Medium"
      else if (amount < 100000) "High"
      else "Critical"
    })

    // 3. Apply UDF using withColumn
    val udfResult = transactionsDF.withColumn(
      "risk_category",
      classifyRisk(col("transaction_amount").cast("double"))
    )

    println("\n========== RISK CATEGORY USING UDF ==========")
    udfResult.show()

    // 4. Apply built-in Spark functions
    val builtInResult = transactionsDF.withColumn(
      "risk_category",
      when(col("transaction_amount") < 10000, "Low")
        .when(col("transaction_amount") < 50000, "Medium")
        .when(col("transaction_amount") < 100000, "High")
        .otherwise("Critical")
    )

    println("\n========== RISK CATEGORY USING BUILT-IN FUNCTIONS ==========")
    builtInResult.show()

    // 5. Register UDF with Spark SQL
    spark.udf.register(
      "classify_risk",
      (amount: Double) => {
        if (amount < 10000) "Low"
        else if (amount < 50000) "Medium"
        else if (amount < 100000) "High"
        else "Critical"
      }
    )

    // 6. Use registered UDF in SQL
    udfResult.createOrReplaceTempView("transactions")

    println("\n========== REGISTERED UDF USING SQL ==========")
    spark.sql(
      """
        SELECT
          transaction_id,
          customer_id,
          transaction_amount,
          classify_risk(CAST(transaction_amount AS DOUBLE)) AS risk_category
        FROM transactions
        ORDER BY transaction_id
      """
    ).show()

    // 7. Customer risk report
    println("\n========== CUSTOMER RISK REPORT ==========")
    udfResult
      .groupBy("customer_id", "risk_category")
      .agg(
        sum("transaction_amount").alias("total_transaction_value"),
        count("*").alias("transaction_count")
      )
      .orderBy("customer_id", "risk_category")
      .show()

    // 8. Compare UDF and built-in functions
    println("\n========== UDF VS BUILT-IN ==========")
    println("UDF: Allows custom Scala logic but is less visible to Catalyst.")
    println("Built-in: Uses Spark SQL expressions that Catalyst can optimize.")

    spark.stop()
  }
}
