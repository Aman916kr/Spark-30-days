import org.apache.spark.sql.SparkSession

object SparkDay4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Day 4 - RDD Creation")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("====================================")
    println("       SPARK DAY 4 - RDD CREATION")
    println("====================================")

    // --------------------------------------------------
    // 1. Create RDD from a collection
    // --------------------------------------------------

    val numbersRDD = sc.parallelize(1 to 10)

    println("\n=== RDD Created From Collection ===")
    numbersRDD.collect().foreach(println)

    // --------------------------------------------------
    // 2. map operation
    // --------------------------------------------------

    val squaredRDD = numbersRDD.map(number => number * number)

    println("\n=== map() - Squares ===")
    squaredRDD.collect().foreach(println)

    // --------------------------------------------------
    // 3. filter operation
    // --------------------------------------------------

    val evenRDD = numbersRDD.filter(number => number % 2 == 0)

    println("\n=== filter() - Even Numbers ===")
    evenRDD.collect().foreach(println)

    // --------------------------------------------------
    // 4. flatMap operation
    // --------------------------------------------------

    val sentencesRDD = sc.parallelize(
      Seq(
        "Spark is fast",
        "RDDs are distributed",
        "Scala works with Spark"
      )
    )

    val wordsRDD = sentencesRDD.flatMap(sentence => sentence.split(" "))

    println("\n=== flatMap() - Words ===")
    wordsRDD.collect().foreach(println)

    // --------------------------------------------------
    // 5. Read transaction records from text file
    // --------------------------------------------------

    val transactionsRDD = sc.textFile("data/transactions.txt")

    println("\n=== Transaction Records ===")
    transactionsRDD.collect().foreach(println)

    // --------------------------------------------------
    // 6. Calculate total sales
    // --------------------------------------------------

    val totalSales = transactionsRDD
      .map { line =>
        val fields = line.split(",")
        val quantity = fields(2).toInt
        val price = fields(3).toDouble
        quantity * price
      }
      .reduce(_ + _)

    println(s"\n=== Total Sales ===")
    println(f"Total Sales: $totalSales%.2f")

    // --------------------------------------------------
    // 7. Inspect partitions
    // --------------------------------------------------

    println("\n=== Partition Information ===")

    println(s"Number of partitions: ${transactionsRDD.getNumPartitions}")

    println(s"Default parallelism: ${sc.defaultParallelism}")

    val partitionSizes = transactionsRDD
      .mapPartitions(iterator => Iterator(iterator.size))
      .collect()

    println("Records in each partition:")
    partitionSizes.foreach(size => println(s"Partition records: $size"))

    // --------------------------------------------------
    // 8. Large customer file simulation
    // --------------------------------------------------

    val customerRDD = sc.textFile("data/transactions.txt", 4)

    println("\n=== Customer File Partition Scenario ===")
    println(s"Requested partitions: ${customerRDD.getNumPartitions}")

    spark.stop()
  }
}
