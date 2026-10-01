import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object Day12CachePersist {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day12 Cache and Persist")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // --------------------------------------------------
    // 1. Transaction Dataset
    // --------------------------------------------------

    val transactions = sc.parallelize(
      Seq(
        (1, "A101", 5000, "SUCCESS"),
        (2, "A102", 3000, "SUCCESS"),
        (3, "A103", -500, "FAILED"),
        (4, "A101", 7000, "SUCCESS"),
        (5, "A104", 2000, "SUCCESS"),
        (6, "A102", -1000, "FAILED"),
        (7, "A105", 6000, "SUCCESS"),
        (8, "A103", 4000, "SUCCESS"),
        (9, "A101", 8000, "SUCCESS"),
        (10, "A104", 3500, "SUCCESS")
      )
    )

    // --------------------------------------------------
    // 2. Clean Transaction Dataset
    // --------------------------------------------------

    val cleanedTransactions = transactions
      .filter(transaction => transaction._4 == "SUCCESS")
      .filter(transaction => transaction._3 > 0)
      .map(transaction =>
        (
          transaction._1,
          transaction._2,
          transaction._3
        )
      )

    // --------------------------------------------------
    // 3. Cache the Cleaned Dataset
    // --------------------------------------------------

    cleanedTransactions.cache()

    println("\n========== CLEANED TRANSACTIONS ==========")

    cleanedTransactions.collect().foreach(println)

    // --------------------------------------------------
    // 4. Report 1 - Total Transaction Amount
    // --------------------------------------------------

    println("\n========== REPORT 1 ==========")

    val totalAmount =
      cleanedTransactions.map(_._3).sum()

    println(s"Total Transaction Amount: $totalAmount")

    // --------------------------------------------------
    // 5. Report 2 - Number of Transactions
    // --------------------------------------------------

    println("\n========== REPORT 2 ==========")

    val transactionCount =
      cleanedTransactions.count()

    println(s"Total Valid Transactions: $transactionCount")

    // --------------------------------------------------
    // 6. Report 3 - Account-wise Total
    // --------------------------------------------------

    println("\n========== REPORT 3 ==========")

    val accountTotals =
      cleanedTransactions
        .map(transaction => (transaction._2, transaction._3))
        .reduceByKey(_ + _)
        .collect()
        .sortBy(_._1)

    accountTotals.foreach {
      case (account, amount) =>
        println(s"$account -> $amount")
    }

    // --------------------------------------------------
    // 7. Cache Information
    // --------------------------------------------------

    println("\n========== CACHE INFORMATION ==========")

    println(
      s"RDD is cached: ${cleanedTransactions.getStorageLevel.useMemory}"
    )

    println(
      s"Storage Level: ${cleanedTransactions.getStorageLevel}"
    )

    // --------------------------------------------------
    // 8. Persist Example
    // --------------------------------------------------

    val persistedTransactions =
      transactions
        .filter(transaction => transaction._4 == "SUCCESS")
        .filter(transaction => transaction._3 > 0)
        .map(transaction =>
          (
            transaction._1,
            transaction._2,
            transaction._3
          )
        )

    persistedTransactions.persist(StorageLevel.MEMORY_ONLY)

    println("\n========== PERSIST EXAMPLE ==========")

    println(
      s"Storage Level: ${persistedTransactions.getStorageLevel}"
    )

    println(
      s"Persisted Transaction Count: ${persistedTransactions.count()}"
    )

    // --------------------------------------------------
    // 9. Storage Level Examples
    // --------------------------------------------------

    println("\n========== STORAGE LEVELS ==========")

    println("MEMORY_ONLY")
    println("MEMORY_AND_DISK")
    println("DISK_ONLY")
    println("MEMORY_ONLY_SER")
    println("MEMORY_AND_DISK_SER")

    // --------------------------------------------------
    // 10. Unpersist
    // --------------------------------------------------

    cleanedTransactions.unpersist()

    persistedTransactions.unpersist()

    println("\n========== CLEANUP ==========")

    println("Cached and persisted RDDs have been unpersisted.")

    sc.stop()
  }
}
