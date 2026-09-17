import org.apache.spark.sql.SparkSession

object SparkDay3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark Day 3")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("=== Spark Day 3 ===")
    println(s"Spark Version: ${spark.version}")
    println(s"Application Name: ${sc.appName}")
    println(s"Master: ${sc.master}")

    // Create an RDD from a collection
    val numbers = sc.parallelize(1 to 10)

    println("\n=== Numbers RDD ===")
    numbers.collect().foreach(println)

    // Read a text file
    val lines = sc.textFile("data/input.txt")

    println("\n=== Text File Contents ===")
    lines.collect().foreach(println)

    spark.stop()
  }
}
