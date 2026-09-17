import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitioningDemo {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------
    // Spark Configuration
    // -----------------------------------------

    val conf = new SparkConf()
      .setAppName("Day10-Partitioning")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")


    // -----------------------------------------
    // 1. Create RDD with too few partitions
    // -----------------------------------------

    val numbers = sc.parallelize(1 to 20, 2)

    println("\n========== ORIGINAL PARTITIONS ==========")

    println(
      s"Number of partitions: ${numbers.getNumPartitions}"
    )


    // -----------------------------------------
    // 2. Inspect partition distribution
    // -----------------------------------------

    println("\n========== ORIGINAL PARTITION DATA ==========")

    numbers
      .mapPartitionsWithIndex {
        case (partitionId, iterator) =>
          Iterator(
            s"Partition $partitionId -> ${iterator.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)


    // -----------------------------------------
    // 3. Repartition
    // -----------------------------------------

    val repartitioned = numbers.repartition(4)

    println("\n========== REPARTITION ==========")

    println(
      s"Partitions after repartition: ${repartitioned.getNumPartitions}"
    )


    // -----------------------------------------
    // 4. Inspect repartitioned data
    // -----------------------------------------

    println("\n========== REPARTITIONED DATA ==========")

    repartitioned
      .mapPartitionsWithIndex {
        case (partitionId, iterator) =>
          Iterator(
            s"Partition $partitionId -> ${iterator.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)


    // -----------------------------------------
    // 5. Coalesce
    // -----------------------------------------

    val coalesced = repartitioned.coalesce(2)

    println("\n========== COALESCE ==========")

    println(
      s"Partitions after coalesce: ${coalesced.getNumPartitions}"
    )


    // -----------------------------------------
    // 6. Pair RDD
    // -----------------------------------------

    val transactions = sc.parallelize(
      Seq(
        ("A101", 5000),
        ("A102", 3000),
        ("A101", 2000),
        ("A103", 7000),
        ("A102", 4000),
        ("A104", 6000),
        ("A101", 1000),
        ("A103", 3000)
      ),
      2
    )

    println("\n========== PAIR RDD ==========")

    transactions.collect().foreach(println)


    // -----------------------------------------
    // 7. partitionBy
    // -----------------------------------------

    val partitionedTransactions =
      transactions.partitionBy(new HashPartitioner(4))

    println("\n========== PARTITIONBY ==========")

    println(
      s"Partitions after partitionBy: " +
        s"${partitionedTransactions.getNumPartitions}"
    )


    // -----------------------------------------
    // 8. Inspect Pair RDD partitions
    // -----------------------------------------

    println("\n========== PARTITIONED TRANSACTIONS ==========")

    partitionedTransactions
      .mapPartitionsWithIndex {
        case (partitionId, iterator) =>
          Iterator(
            s"Partition $partitionId -> ${iterator.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)


    // -----------------------------------------
    // 9. Account Aggregation
    // -----------------------------------------

    val accountTotals =
      partitionedTransactions.reduceByKey(_ + _)

    println("\n========== ACCOUNT TOTALS ==========")

    accountTotals
      .collect()
      .sortBy(_._1)
      .foreach(println)


    // -----------------------------------------
    // 10. Optimization Scenario
    // -----------------------------------------

    println("\n========== OPTIMIZATION SCENARIO ==========")

    println("Initial partitions: 2")
    println("Dataset has too few partitions.")
    println("Increasing partitions to 4 using repartition().")
    println("More partitions allow more parallel processing.")

    println("\nOptimization:")
    println("2 partitions -> 4 partitions")


    // -----------------------------------------
    // 11. Explain coalesce
    // -----------------------------------------

    println("\n========== COALESCE SCENARIO ==========")

    println("Coalesce reduces the number of partitions.")
    println("It is useful when the dataset has too many partitions.")
    println("Example: 4 partitions -> 2 partitions")


    // -----------------------------------------
    // Stop Spark
    // -----------------------------------------

    sc.stop()
  }
}
