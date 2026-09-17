object StudentGrade {

  // 1. val, var, lazy val

  val college = "ABC College"

  var studentCount = 3

  lazy val message = {
    println("Lazy value is calculated now")
    "Student Grade Processor Started"
  }


  // 2. Immutable collections

  val students = List(
    "Aman",
    "Rahul",
    "Priya"
  )

  val marks = List(
    85,
    72,
    91
  )


  // 3. List

  val studentList = List("Aman", "Rahul", "Priya")


  // 4. Vector

  val studentVector = Vector("Aman", "Rahul", "Priya")


  // 5. Set

  val subjects = Set(
    "Scala",
    "Spark",
    "Kafka",
    "Scala"
  )


  // 6. Map

  val studentMarks = Map(
    "Aman" -> 85,
    "Rahul" -> 72,
    "Priya" -> 91
  )


  // 7. Trait

  trait Logger {

    def log(message: String): Unit = {
      println(s"[LOG] $message")
    }

  }


  class StudentProcessor extends Logger {

    def process(student: String, mark: Int): Unit = {
      log(s"Processing student: $student, Mark: $mark")
    }

  }


  class GradeReporter extends Logger {

    def report(student: String, grade: String): Unit = {
      log(s"$student received Grade $grade")
    }

  }


  // 8. Grade calculation

  def calculateGrade(mark: Int): String = {

    if (mark >= 90)
      "A"
    else if (mark >= 80)
      "B"
    else if (mark >= 70)
      "C"
    else if (mark >= 60)
      "D"
    else
      "F"
  }


  // 9. Main program

  def main(args: Array[String]): Unit = {

    println("====================================")
    println("        STUDENT GRADE")
    println("     Student Grade Processor")
    println("====================================")


    // val

    println("\n--- VAL ---")

    println(s"College: $college")


    // var

    println("\n--- VAR ---")

    println(s"Initial student count: $studentCount")

    studentCount = 5

    println(s"Updated student count: $studentCount")


    // lazy val

    println("\n--- LAZY VAL ---")

    println("Before accessing lazy value")

    println(message)

    println("After accessing lazy value")


    // List

    println("\n--- LIST ---")

    println(studentList)

    println(s"First student: ${studentList.head}")


    // Vector

    println("\n--- VECTOR ---")

    println(studentVector)

    println(s"Second student: ${studentVector(1)}")


    // Set

    println("\n--- SET ---")

    println(subjects)


    // Map

    println("\n--- MAP ---")

    println(studentMarks)

    println(s"Aman's marks: ${studentMarks("Aman")}")


    // Immutable collection

    println("\n--- IMMUTABLE COLLECTION ---")

    val updatedStudents = students :+ "Vikram"

    println(s"Original: $students")

    println(s"New: $updatedStudents")


    // For-comprehension with yield

    println("\n--- FOR COMPREHENSION ---")

    val studentResults = for {
      student <- students
      mark <- marks
    } yield s"$student scored $mark"


    studentResults.take(5).foreach(println)


    // Trait

    println("\n--- TRAIT LOGGER ---")

    val processor = new StudentProcessor

    processor.process("Aman", 85)

    val reporter = new GradeReporter

    reporter.report("Aman", calculateGrade(85))


    // Student Grade Processor

    println("\n--- STUDENT GRADE PROCESSOR ---")

    val grades = studentMarks.map {

      case (student, mark) =>
        val grade = calculateGrade(mark)

        s"$student -> Marks: $mark -> Grade: $grade"
    }


    grades.foreach(println)


    println("\n====================================")
    println("       STUDENT GRADE COMPLETE")
    println("====================================")
  }

}
