import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionsAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Window Functions Analysis")
      .master("local[*]")
      .getOrCreate()

    val studentsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/student_scores.csv")

    val policiesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .csv("data/customer_policies.csv")

    // 1. row_number, rank and dense_rank per course
    val courseWindow = Window
      .partitionBy("course")
      .orderBy(desc("score"))

    println("=== Student Rankings ===")
    val rankedStudents = studentsDF
      .withColumn("row_number", row_number().over(courseWindow))
      .withColumn("rank", rank().over(courseWindow))
      .withColumn("dense_rank", dense_rank().over(courseWindow))

    rankedStudents
      .orderBy("course", "row_number")
      .show()

    // 2. Top 3 students per course
    println("=== Top 3 Students Per Course ===")
    rankedStudents
      .filter(col("row_number") <= 3)
      .orderBy("course", "row_number")
      .show()

    // 3. Latest policy per customer
    val latestPolicyWindow = Window
      .partitionBy("customer_id")
      .orderBy(desc("policy_date"), desc("policy_id"))

    println("=== Latest Policy Per Customer ===")
    policiesDF
      .withColumn("row_number", row_number().over(latestPolicyWindow))
      .filter(col("row_number") === 1)
      .drop("row_number")
      .orderBy("customer_id")
      .show()

    // 4. lag and lead policy premiums
    val policyHistoryWindow = Window
      .partitionBy("customer_id")
      .orderBy("policy_date")

    println("=== Policy History With Lag and Lead ===")
    policiesDF
      .withColumn("previous_premium", lag("premium", 1).over(policyHistoryWindow))
      .withColumn("next_premium", lead("premium", 1).over(policyHistoryWindow))
      .orderBy("customer_id", "policy_date")
      .show()

    spark.stop()
  }
}
