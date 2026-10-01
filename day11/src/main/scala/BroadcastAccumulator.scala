
import org.apache.spark.{SparkConf, SparkContext}

object Day11BroadcastAccumulator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day11 Broadcast and Accumulators")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // --------------------------------------------------
    // 1. Small Product Master Table
    // --------------------------------------------------

    val productMaster = Map(
      101 -> "Laptop",
      102 -> "Mouse",
      103 -> "Keyboard",
      104 -> "Monitor"
    )

    // --------------------------------------------------
    // 2. Broadcast Product Master
    // --------------------------------------------------

    val broadcastProducts = sc.broadcast(productMaster)

    // --------------------------------------------------
    // 3. Accumulator for Bad Records
    // --------------------------------------------------

    val badRecords = sc.longAccumulator("Bad Records")

    // --------------------------------------------------
    // 4. Transaction Data
    // --------------------------------------------------

    val transactions = sc.parallelize(
      Seq(
        (1, 101, 50000),
        (2, 102, 1000),
        (3, 999, 2000),
        (4, 103, 1500),
        (5, 888, 3000),
        (6, 104, 12000)
      )
    )

    // --------------------------------------------------
    // 5. Validate Transactions
    // --------------------------------------------------

    val validTransactions = transactions.filter { transaction =>

      val transactionId = transaction._1
      val productId = transaction._2
      val amount = transaction._3

      if (broadcastProducts.value.contains(productId)) {

        true

      } else {

        badRecords.add(1)

        println(
          s"Bad Transaction -> ID: $transactionId, Product: $productId, Amount: $amount"
        )

        false
      }
    }

    // --------------------------------------------------
    // 6. Display Valid Transactions
    // --------------------------------------------------

    println("\n========== VALID TRANSACTIONS ==========")

    validTransactions.collect().foreach { transaction =>
      println(
        s"Transaction ID: ${transaction._1}, " +
        s"Product ID: ${transaction._2}, " +
        s"Amount: ${transaction._3}"
      )
    }

    // --------------------------------------------------
    // 7. Display Accumulator Result
    // --------------------------------------------------

    println("\n========== SUMMARY ==========")

    println(s"Total Transactions : ${transactions.count()}")

    println(s"Valid Transactions : ${validTransactions.count()}")

    println(s"Bad Transactions   : ${badRecords.value}")

    // --------------------------------------------------
    // 8. Stop Spark
    // --------------------------------------------------

    sc.stop()
  }
}
