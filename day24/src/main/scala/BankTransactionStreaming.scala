import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BankTransactionStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day24-Bank-Transaction-Streaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Required for stateful operations
    ssc.checkpoint("checkpoint")

    // Input format: accountId,transactionId
    val transactions = ssc.socketTextStream("localhost", 9999)

    // Parse incoming transactions
    val parsed = transactions.flatMap { line =>
      val parts = line.split(",")
      if (parts.length == 2) {
        Some((parts(0).trim, parts(1).trim))
      } else {
        None
      }
    }

    // STATELESS: count transactions in each batch only
    val batchCounts = parsed
      .map { case (account, _) => (account, 1) }
      .reduceByKey(_ + _)

    batchCounts.foreachRDD { rdd =>
      println("\n===== CURRENT BATCH COUNTS =====")
      if (rdd.isEmpty()) {
        println("No transactions in this batch.")
      } else {
        rdd.collect().foreach {
          case (account, count) =>
            println(s"$account -> $count")
        }
      }
    }

    // STATEFUL: maintain cumulative counts across batches
    val runningCounts = parsed
      .map { case (account, _) => (account, 1) }
      .updateStateByKey[Int] {
        (newCounts: Seq[Int], previousState: Option[Int]) =>
          Some(newCounts.sum + previousState.getOrElse(0))
      }

    runningCounts.foreachRDD { rdd =>
      println("\n===== ACCUMULATED RUNNING COUNTS =====")
      if (rdd.isEmpty()) {
        println("No account counts available.")
      } else {
        rdd.collect().foreach {
          case (account, count) =>
            println(s"$account -> $count")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
