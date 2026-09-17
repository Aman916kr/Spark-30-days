import org.apache.spark.{SparkConf, SparkContext}

object DAGDemo {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------
    // Spark Configuration
    // -----------------------------------------

    val conf = new SparkConf()
      .setAppName("Day8-DAG-Execution")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")


    // -----------------------------------------
    // Create RDD with 4 partitions
    // -----------------------------------------

    val numbers = sc.parallelize(1 to 20, 4)

    println("\n========== PARTITIONS ==========")

    println(s"Number of partitions: ${numbers.getNumPartitions}")


    // -----------------------------------------
    // Narrow Transformations
    // -----------------------------------------

    val doubled = numbers.map(_ * 2)

    val filtered = doubled.filter(_ > 20)


    // -----------------------------------------
    // Wide Transformation
    // -----------------------------------------

    val keyValueRDD = filtered.map { x =>
      (x % 3, x)
    }

    val reduced = keyValueRDD.reduceByKey(_ + _)


    // -----------------------------------------
    // Another Transformation
    // -----------------------------------------

    val finalResult = reduced.map {
      case (key, value) =>
        (key, value * 10)
    }


    // -----------------------------------------
    // Show Lineage
    // -----------------------------------------

    println("\n========== RDD LINEAGE ==========")

    println(finalResult.toDebugString)


    // -----------------------------------------
    // Action 1
    // -----------------------------------------

    println("\n========== ACTION: COLLECT ==========")

    finalResult.collect()
      .sortBy(_._1)
      .foreach(println)


    // -----------------------------------------
    // Action 2
    // -----------------------------------------

    println("\n========== ACTION: COUNT ==========")

    println(s"Result count: ${finalResult.count()}")


    // -----------------------------------------
    // Stage Prediction
    // -----------------------------------------

    println("\n========== STAGE ANALYSIS ==========")

    println("numbers")
    println("   ↓ map")
    println("doubled")
    println("   ↓ filter")
    println("filtered")
    println("   ↓ map")
    println("keyValueRDD")
    println("   ↓ reduceByKey")
    println("SHUFFLE BOUNDARY")
    println("   ↓")
    println("reduced")
    println("   ↓ map")
    println("finalResult")

    println("\nPredicted stages for each action: 2")
    println("Stage 0: map → filter → map")
    println("Stage 1: reduceByKey → map")


    // -----------------------------------------
    // Narrow vs Wide
    // -----------------------------------------

    println("\n========== NARROW VS WIDE ==========")

    println("map       -> Narrow")
    println("filter    -> Narrow")
    println("map       -> Narrow")
    println("reduceByKey -> Wide")
    println("map       -> Narrow")


    // -----------------------------------------
    // Job / Stage / Task / Partition
    // -----------------------------------------

    println("\n========== SPARK EXECUTION MODEL ==========")

    println("Action creates a Job.")
    println("Job is divided into Stages.")
    println("Stages are divided into Tasks.")
    println("Tasks operate on Partitions.")


    // -----------------------------------------
    // Stop Spark
    // -----------------------------------------

    sc.stop()
  }
}
