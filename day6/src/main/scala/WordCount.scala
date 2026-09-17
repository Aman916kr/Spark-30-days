import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day6-Word-Count")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // -----------------------------------------
    // Application logs
    // -----------------------------------------

    val logs = Seq(
      "INFO Application started successfully.",
      "ERROR Database connection failed.",
      "INFO User logged in successfully.",
      "WARNING Disk space is low.",
      "ERROR Database connection failed.",
      "INFO Processing request.",
      "ERROR File not found.",
      "INFO Application started successfully.",
      "ERROR Network connection failed.",
      "INFO User logged out successfully.",
      "WARNING Memory usage is high.",
      "ERROR File not found."
    )

    val logRDD = sc.parallelize(logs)


    // -----------------------------------------
    // 1. Classic Word Count
    // -----------------------------------------

    val words = logRDD
      .flatMap(line => line.split(" "))

    val wordPairs = words
      .map(word => (word, 1))

    val wordCounts = wordPairs
      .reduceByKey(_ + _)

    println("\n========== CLASSIC WORD COUNT ==========")

    wordCounts.collect()
      .sortBy(-_._2)
      .foreach(println)


    // -----------------------------------------
    // 2. Case-Insensitive Word Count
    // -----------------------------------------

    val cleanWords = logRDD
      .flatMap(line => line
        .toLowerCase
        .replaceAll("[^a-z0-9\\s]", "")
        .split("\\s+")
      )
      .filter(_.nonEmpty)

    val caseInsensitiveCounts = cleanWords
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("\n========== CASE-INSENSITIVE WORD COUNT ==========")

    caseInsensitiveCounts
      .collect()
      .sortBy(-_._2)
      .foreach(println)


    // -----------------------------------------
    // 3. Top 10 Most Frequent Words
    // -----------------------------------------

    val top10Words = caseInsensitiveCounts
      .sortBy {
        case (word, count) =>
          (-count, word)
      }
      .take(10)

    println("\n========== TOP 10 MOST FREQUENT WORDS ==========")

    top10Words.foreach(println)


    // -----------------------------------------
    // 4. Total Number of Words
    // -----------------------------------------

    val totalWords = cleanWords.count()

    println("\n========== TOTAL WORDS ==========")
    println(s"Total words: $totalWords")


    // -----------------------------------------
    // Stop Spark
    // -----------------------------------------

    sc.stop()
  }
}
