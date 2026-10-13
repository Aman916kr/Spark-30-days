import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BankingMonitoring {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day27-RealTimeBanking")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    // 1. Branch risk reference data
    val branchRisk = Map(
      "BR001" -> "HIGH",
      "BR002" -> "LOW",
      "BR003" -> "MEDIUM"
    )

    val broadcastBranchRisk =
      ssc.sparkContext.broadcast(branchRisk)

    // 2. Transaction input
    // Format:
    // transactionId,timestamp,accountId,amount,branchId,transactionType
    val input = ssc.socketTextStream("localhost", 9999)

    // 3. Validate and parse transactions
    val transactions = input.flatMap { line =>
      val p = line.split(",")

      if (p.length == 6) {
        try {
          val transactionId = p(0).trim
          val timestamp = p(1).trim
          val accountId = p(2).trim
          val amount = p(3).trim.toDouble
          val branchId = p(4).trim
          val transactionType = p(5).trim.toUpperCase

          if (
            transactionId.nonEmpty &&
            timestamp.nonEmpty &&
            accountId.nonEmpty &&
            amount > 0 &&
            branchId.nonEmpty &&
            Set("DEPOSIT", "WITHDRAWAL").contains(transactionType)
          ) {
            Some((
              transactionId,
              timestamp,
              accountId,
              amount,
              branchId,
              transactionType
            ))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)

    // 4. Aggregate transaction count and amount by account
    val accountTotals = transactions
      .map(t => (t._3, (1L, t._4)))
      .reduceByKey {
        case ((count1, amount1), (count2, amount2)) =>
          (count1 + count2, amount1 + amount2)
      }

    accountTotals.foreachRDD { rdd =>
      rdd.collect().foreach {
        case (accountId, (count, amount)) =>
          println(
            f"ACCOUNT SUMMARY account=$accountId count=$count amount=$amount%.2f"
          )
      }
    }

    // 5. Detect suspicious bursts within a 60-second window
    val suspiciousBursts = transactions
      .map(t => (t._3, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(60),
        Seconds(5)
      )

    suspiciousBursts.foreachRDD { rdd =>
      rdd.filter(_._2 >= 3).collect().foreach {
        case (accountId, count) =>
          println(
            s"SUSPICIOUS BURST account=$accountId count=$count in last 60 seconds"
          )
      }
    }

    // 6. Branch risk analysis using broadcast reference data
    transactions.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val riskMap = broadcastBranchRisk.value

        partition.foreach { t =>
          val risk = riskMap.getOrElse(t._5, "UNKNOWN")

          println(
            f"TRANSACTION id=${t._1} account=${t._3} " +
            f"amount=${t._4}%.2f branch=${t._5} " +
            s"type=${t._6} risk=$risk"
          )

          if (risk == "HIGH" && t._4 >= 5000) {
            println(
              s"HIGH RISK TRANSACTION id=${t._1} " +
              s"account=${t._3} amount=${t._4}"
            )
          }
        }
      }
    }

    // 7. Repartition transactions for parallel processing
    val partitionedTransactions = transactions.transform { rdd =>
      rdd.repartition(4)
    }

    partitionedTransactions.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println(
          s"PARTITIONING INFO partitions=${rdd.getNumPartitions}"
        )
      }
    }

    // 8. Start the streaming application
    ssc.start()
    ssc.awaitTermination()
  }
}
