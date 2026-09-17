import org.apache.spark.{SparkConf, SparkContext}

object LineageDemo {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------
    // 1. Spark Configuration
    // -----------------------------------------

    val conf = new SparkConf()
      .setAppName("Day7-Immutability-Lineage")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")


    // -----------------------------------------
    // 2. Create Base RDD
    // -----------------------------------------

    val numbers = sc.parallelize(
      1 to 10,
      2
    )

    println("\n========== ORIGINAL RDD ==========")

    numbers.collect().foreach(println)


    // -----------------------------------------
    // 3. Transformation Chain
    // -----------------------------------------

    val doubled = numbers.map(_ * 2)

    val filtered = doubled.filter(_ > 10)

    val squared = filtered.map(x => x * x)


    // -----------------------------------------
    // 4. Show Transformation Results
    // -----------------------------------------

    println("\n========== DOUBLED RDD ==========")

    doubled.collect().foreach(println)


    println("\n========== FILTERED RDD ==========")

    filtered.collect().foreach(println)


    println("\n========== SQUARED RDD ==========")

    squared.collect().foreach(println)


    // -----------------------------------------
    // 5. Show Lineage
    // -----------------------------------------

    println("\n========== RDD LINEAGE ==========")

    println(squared.toDebugString)


    // -----------------------------------------
    // 6. RDD IDs
    // -----------------------------------------

    println("\n========== RDD IDs ==========")

    println(s"Original RDD ID: ${numbers.id}")
    println(s"Doubled RDD ID: ${doubled.id}")
    println(s"Filtered RDD ID: ${filtered.id}")
    println(s"Squared RDD ID: ${squared.id}")


    // -----------------------------------------
    // 7. Immutability Demonstration
    // -----------------------------------------

    println("\n========== IMMUTABILITY ==========")

    println("Original RDD remains unchanged.")

    println("Original RDD:")
    numbers.collect().foreach(println)

    println("Transformed RDD:")
    squared.collect().foreach(println)


    // -----------------------------------------
    // 8. Fault Tolerance Concept
    // -----------------------------------------

    println("\n========== FAULT TOLERANCE ==========")

    println("If a partition of the final RDD is lost,")
    println("Spark uses lineage to recompute the lost partition.")

    println("\nLineage:")
    println("numbers")
    println("   ↓ map(_ * 2)")
    println("doubled")
    println("   ↓ filter(_ > 10)")
    println("filtered")
    println("   ↓ map(x => x * x)")
    println("squared")

    println("\nConceptual recomputation:")
    println("Lost squared partition")
    println("        ↓")
    println("Recompute filtered partition")
    println("        ↓")
    println("Recompute doubled partition")
    println("        ↓")
    println("Read required data from original numbers partition")


    // -----------------------------------------
    // 9. Final Result
    // -----------------------------------------

    println("\n========== FINAL RESULT ==========")

    println(s"Final record count: ${squared.count()}")

    println(s"Final sum: ${squared.reduce(_ + _)}")


    // -----------------------------------------
    // Stop Spark
    // -----------------------------------------

    sc.stop()
  }
}
