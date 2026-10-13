import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowOperations {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day25-Window-Operations")
      .setMaster("local[*]")

    // Spark processes incoming data every 5 seconds
    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for stateful window operations
    ssc.checkpoint("checkpoint")

    // Input format: transactionId,accountId,amount
    val transactions = ssc.socketTextStream("localhost", 9999)

    val parsed = transactions.flatMap { line =>
      val parts = line.split(",")

      if (parts.length == 3) {
        try {
          val transactionId = parts(0).trim
          val accountId = parts(1).trim
          val amount = parts(2).trim.toDouble

          if (transactionId.nonEmpty &&
              accountId.nonEmpty &&
              amount > 0) {
            Some((transactionId, accountId, amount))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // 1. Stateless: count transactions in each 5-second batch
    val batchCounts = parsed
      .map { case (_, _, _) => 1 }
      .count()

    batchCounts.foreachRDD { rdd =>
      println(s"\nCurrent batch transaction count: ${rdd.count()}")
    }

    // 2. Window operation: total transactions in the last 10 minutes
    val transactionCounts = parsed
      .map { case (_, _, _) => 1 }

    val rollingCount = transactionCounts.countByWindow(
      Seconds(600),
      Seconds(5)
    )

    rollingCount.foreachRDD { rdd =>
      rdd.collect().foreach { count =>
        println(s"Rolling 10-minute transaction count: $count")

        if (count >= 20) {
          println("ALERT: Transaction spike detected!")
        }
      }
    }

    // 3. Rolling sales totals by account
    val accountSales = parsed.map {
      case (_, accountId, amount) => (accountId, amount)
    }

    val rollingSales = accountSales.reduceByKeyAndWindow(
      (a: Double, b: Double) => a + b,
      Seconds(600),
      Seconds(5)
    )

    rollingSales.foreachRDD { rdd =>
      println("\n===== ROLLING 10-MINUTE SALES BY ACCOUNT =====")

      rdd.collect().foreach {
        case (account, total) =>
          println(f"$account -> $total%.2f")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
