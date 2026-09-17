import org.apache.spark.{SparkConf, SparkContext}

object PairRDDDemo {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------
    // Spark Configuration
    // -----------------------------------------

    val conf = new SparkConf()
      .setAppName("Day9-Pair-RDD")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")


    // -----------------------------------------
    // Bank Transactions
    // -----------------------------------------

    val transactions = Seq(
      ("A101", "Sales", "Laptop", 60000),
      ("A102", "Sales", "Phone", 25000),
      ("A101", "Sales", "Mouse", 2000),
      ("A103", "HR", "Chair", 5000),
      ("A102", "Sales", "Laptop", 60000),
      ("A104", "IT", "Monitor", 15000),
      ("A103", "HR", "Table", 8000),
      ("A101", "Sales", "Keyboard", 3000),
      ("A104", "IT", "Laptop", 60000),
      ("A102", "Sales", "Mouse", 2000)
    )

    val transactionRDD = sc.parallelize(transactions)


    // -----------------------------------------
    // 1. Create Pair RDD
    // -----------------------------------------

    val accountAmounts = transactionRDD.map {
      case (accountId, department, product, amount) =>
        (accountId, amount)
    }

    println("\n========== KEY-VALUE RDD ==========")

    accountAmounts.collect().foreach(println)


    // -----------------------------------------
    // 2. reduceByKey
    // -----------------------------------------

    val accountTotals = accountAmounts
      .reduceByKey(_ + _)

    println("\n========== REDUCEBYKEY: ACCOUNT TOTALS ==========")

    accountTotals
      .collect()
      .sortBy(_._1)
      .foreach(println)


    // -----------------------------------------
    // 3. groupByKey
    // -----------------------------------------

    val accountGrouped = accountAmounts
      .groupByKey()

    println("\n========== GROUPBYKEY: ACCOUNT TRANSACTIONS ==========")

    accountGrouped
      .collect()
      .sortBy(_._1)
      .foreach {
        case (account, amounts) =>
          println(s"$account -> ${amounts.mkString(", ")}")
      }


    // -----------------------------------------
    // 4. mapValues
    // -----------------------------------------

    val accountWithTax = accountTotals
      .mapValues(amount => amount * 0.10)

    println("\n========== MAPVALUES: ACCOUNT TAX ==========")

    accountWithTax
      .collect()
      .sortBy(_._1)
      .foreach(println)


    // -----------------------------------------
    // 5. Revenue by Product
    // -----------------------------------------

    val productRevenue = transactionRDD
      .map {
        case (_, _, product, amount) =>
          (product, amount)
      }
      .reduceByKey(_ + _)

    println("\n========== REVENUE BY PRODUCT ==========")

    productRevenue
      .collect()
      .sortBy(_._1)
      .foreach(println)


    // -----------------------------------------
    // 6. Revenue by Department
    // -----------------------------------------

    val departmentRevenue = transactionRDD
      .map {
        case (_, department, _, amount) =>
          (department, amount)
      }
      .reduceByKey(_ + _)

    println("\n========== REVENUE BY DEPARTMENT ==========")

    departmentRevenue
      .collect()
      .sortBy(_._1)
      .foreach(println)


    // -----------------------------------------
    // 7. Compare reduceByKey and groupByKey
    // -----------------------------------------

    val reduceStart = System.nanoTime()

    accountAmounts
      .reduceByKey(_ + _)
      .collect()

    val reduceEnd = System.nanoTime()

    val groupStart = System.nanoTime()

    accountAmounts
      .groupByKey()
      .mapValues(_.sum)
      .collect()

    val groupEnd = System.nanoTime()

    val reduceTime =
      (reduceEnd - reduceStart) / 1000000.0

    val groupTime =
      (groupEnd - groupStart) / 1000000.0

    println("\n========== PERFORMANCE COMPARISON ==========")

    println(f"reduceByKey time: $reduceTime%.3f ms")
    println(f"groupByKey time:  $groupTime%.3f ms")

    println("\nreduceByKey performs local aggregation before shuffle.")
    println("groupByKey transfers all values for each key before aggregation.")


    // -----------------------------------------
    // 8. Bank Transaction Scenario
    // -----------------------------------------

    println("\n========== BANK TRANSACTION ANALYSIS ==========")

    println("Transactions aggregated by Account ID:")

    accountTotals
      .collect()
      .sortBy(_._1)
      .foreach {
        case (account, total) =>
          println(s"$account -> ₹$total")
      }


    // -----------------------------------------
    // 9. Overall Revenue
    // -----------------------------------------

    val totalRevenue = transactionRDD
      .map {
        case (_, _, _, amount) =>
          amount
      }
      .reduce(_ + _)

    println("\n========== TOTAL REVENUE ==========")

    println(s"Total revenue: ₹$totalRevenue")


    // -----------------------------------------
    // Stop Spark
    // -----------------------------------------

    sc.stop()
  }
}
