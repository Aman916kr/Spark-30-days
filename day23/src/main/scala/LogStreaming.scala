import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day23-DStreams-Log-Analysis")
      .setMaster("local[*]")

    // Process data in 5-second micro-batches
    val ssc = new StreamingContext(conf, Seconds(5))

    // Read log lines from a TCP socket
    val logStream = ssc.socketTextStream("localhost", 9999)

    // flatMap: split each line into words
    val words = logStream.flatMap(_.split("\\s+"))

    // map: convert each word into (word, 1)
    val wordPairs = words.map(word => (word, 1))

    // filter: keep only ERROR tokens
    val errors = wordPairs.filter {
      case (word, _) => word.equalsIgnoreCase("ERROR")
    }

    // Count ERROR tokens in each micro-batch
    val errorCounts = errors.reduceByKey(_ + _)

    errorCounts.foreachRDD { rdd =>
      println("\n========== ERROR COUNT: LAST 5-SECOND BATCH ==========")
      if (rdd.isEmpty()) {
        println("No ERROR messages in this batch.")
      } else {
        rdd.collect().foreach {
          case (level, count) =>
            println(s"$level: $count")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
