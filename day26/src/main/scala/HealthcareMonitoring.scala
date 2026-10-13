import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

object HealthcareMonitoring {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day26-Healthcare-Monitoring")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    // Illustrative thresholds for this learning exercise only
    val thresholds = ssc.sparkContext.broadcast(
      Map(
        "heartRateMin" -> 50.0,
        "heartRateMax" -> 120.0,
        "spo2Min" -> 92.0,
        "temperatureMax" -> 38.0
      )
    )

    val abnormalCounter: LongAccumulator =
      ssc.sparkContext.longAccumulator("Abnormal Vital Readings")

    // Input: patientId,timestamp,heartRate,spo2,temperature
    val input = ssc.socketTextStream("localhost", 9999)

    val vitals = input.flatMap { line =>
      val parts = line.split(",")

      if (parts.length == 5) {
        try {
          val patientId = parts(0).trim
          val timestamp = parts(1).trim
          val heartRate = parts(2).trim.toDouble
          val spo2 = parts(3).trim.toDouble
          val temperature = parts(4).trim.toDouble

          if (
            patientId.nonEmpty &&
            timestamp.nonEmpty &&
            heartRate > 0 &&
            spo2 >= 0 && spo2 <= 100 &&
            temperature > 0
          ) {
            Some((
              patientId,
              timestamp,
              heartRate,
              spo2,
              temperature
            ))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // Detect abnormal readings using broadcast thresholds
    val abnormal = vitals.flatMap { vital =>
      val limits = thresholds.value

      val reasons = List(
        if (vital._3 < limits("heartRateMin") ||
            vital._3 > limits("heartRateMax"))
          Some("ABNORMAL_HEART_RATE") else None,

        if (vital._4 < limits("spo2Min"))
          Some("LOW_SPO2") else None,

        if (vital._5 > limits("temperatureMax"))
          Some("HIGH_TEMPERATURE") else None
      ).flatten
      if (reasons.nonEmpty) {
        abnormalCounter.add(1)
        Some((vital._1, vital._2, reasons.mkString("|")))
      } else {
        None
      }
    }

    // Print individual abnormal-vital alerts
    abnormal.foreachRDD { rdd =>
      rdd.collect().foreach {
        case (patientId, timestamp, reason) =>
          println(
            s"ALERT patient=$patientId time=$timestamp reason=$reason"
          )
      }
    }

    // Track abnormal readings per patient in a 60-second window.
    val repeatedAlerts = abnormal
      .map { case (patientId, _, _) => (patientId, 1) }
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(60),
        Seconds(5)
      )

    repeatedAlerts.foreachRDD { rdd =>
      rdd.filter(_._2 >= 3).collect().foreach {
        case (patientId, count) =>
          println(
            s"REPEATED ABNORMAL READINGS patient=$patientId count=$count"
          )
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
