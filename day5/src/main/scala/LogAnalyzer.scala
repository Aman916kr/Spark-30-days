import org.apache.spark.{SparkConf, SparkContext}

object LogAnalyzer {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------
    // 1. Spark Configuration
    // -----------------------------------------

    val conf = new SparkConf()
      .setAppName("Day5-Log-Analyzer")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")


    // -----------------------------------------
    // 2. Create Log RDDs
    // -----------------------------------------

    val logs1 = Seq(
      "INFO Application started",
      "ERROR Database connection failed",
      "INFO User logged in",
      "WARNING Disk space is low",
      "ERROR File not found",
      "INFO Processing request",
      "ERROR Database connection failed"
    )

    val logs2 = Seq(
      "INFO Application started",
      "ERROR Network timeout",
      "INFO User logged out",
      "WARNING Memory usage high",
      "ERROR File not found"
    )

    val logRDD1 = sc.parallelize(logs1)
    val logRDD2 = sc.parallelize(logs2)


    // -----------------------------------------
    // 3. MAP Transformation
    // -----------------------------------------

    val logLevels = logRDD1.map { log =>
      log.split(" ")(0)
    }

    println("\n========== MAP ==========")

    logLevels.collect().foreach(println)


    // -----------------------------------------
    // 4. FILTER Transformation
    // -----------------------------------------

    val errorLogs = logRDD1.filter { log =>
      log.startsWith("ERROR")
    }

    println("\n========== FILTER: ERROR LOGS ==========")

    errorLogs.collect().foreach(println)


    // -----------------------------------------
    // 5. FLATMAP Transformation
    // -----------------------------------------

    val words = logRDD1.flatMap { log =>
      log.split(" ")
    }

    println("\n========== FLATMAP: WORDS ==========")

    words.take(15).foreach(println)


    // -----------------------------------------
    // 6. DISTINCT Transformation
    // -----------------------------------------

    val distinctLogs = logRDD1.distinct()

    println("\n========== DISTINCT LOGS ==========")

    distinctLogs.collect().foreach(println)


    // -----------------------------------------
    // 7. UNION Transformation
    // -----------------------------------------

    val combinedLogs = logRDD1.union(logRDD2)

    println("\n========== UNION ==========")

    combinedLogs.collect().foreach(println)


    // -----------------------------------------
    // 8. Count ERROR messages
    // -----------------------------------------

    val errorCount = combinedLogs
      .filter(_.startsWith("ERROR"))
      .count()

    println("\n========== ERROR COUNT ==========")

    println(s"Total ERROR messages: $errorCount")


    // -----------------------------------------
    // 9. COUNT Action
    // -----------------------------------------

    println("\n========== COUNT ==========")

    println(s"Total logs: ${combinedLogs.count()}")


    // -----------------------------------------
    // 10. COLLECT Action
    // -----------------------------------------

    println("\n========== COLLECT ==========")

    combinedLogs.collect().foreach(println)


    // -----------------------------------------
    // 11. FIRST Action
    // -----------------------------------------

    println("\n========== FIRST ==========")

    println(combinedLogs.first())


    // -----------------------------------------
    // 12. TAKE Action
    // -----------------------------------------

    println("\n========== TAKE 3 ==========")

    combinedLogs.take(3).foreach(println)


    // -----------------------------------------
    // 13. REDUCE Action
    // -----------------------------------------

    val numbers = sc.parallelize(Seq(10, 20, 30, 40, 50))

    val total = numbers.reduce(_ + _)

    println("\n========== REDUCE ==========")

    println(s"Sum: $total")


    // -----------------------------------------
    // 14. Transformation vs Action Example
    // -----------------------------------------

    val lazyErrors = combinedLogs.filter(_.startsWith("ERROR"))

    println("\n========== LAZY TRANSFORMATION ==========")

    println("filter() has created a new RDD.")
    println("No computation happens until an action is called.")

    val result = lazyErrors.count()

    println(s"Action count() triggered execution.")
    println(s"ERROR count: $result")


    // -----------------------------------------
    // 15. Stop Spark
    // -----------------------------------------

    sc.stop()
  }
}
