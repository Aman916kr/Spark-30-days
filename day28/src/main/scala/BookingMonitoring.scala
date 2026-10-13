import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession

object BookingMonitoring {

  case class BookingEvent(
    eventId: String,
    timestamp: String,
    resourceId: String,
    bookingId: String,
    eventType: String,
    quantity: Int
  )

  case class BookingState(
    resourceId: String,
    quantity: Int,
    active: Boolean
  )

  def updateBookingState(
    events: Seq[BookingEvent],
    previousState: Option[BookingState]
  ): Option[BookingState] = {

    var state = previousState

    events.foreach { event =>
      event.eventType match {

        case "BOOK" =>
          state match {
            case None =>
              state = Some(
                BookingState(
                  event.resourceId,
                  event.quantity,
                  active = true
                )
              )

            case Some(existing) if !existing.active =>
              state = Some(
                BookingState(
                  event.resourceId,
                  event.quantity,
                  active = true
                )
              )

            case _ =>
              // Ignore duplicate booking while already active
          }

        case "CANCEL" =>
          state match {
            case Some(existing)
                if existing.active &&
                   existing.resourceId == event.resourceId =>
              state = Some(existing.copy(active = false))

            case _ =>
              // Ignore cancellation without an active booking
          }

        case _ =>
      }
    }

    state
  }

  def main(args: Array[String]): Unit = {

    // 1. Spark configuration
    val conf = new SparkConf()
      .setAppName("Day28-RealTimeBooking")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // 2. Broadcast capacity reference data
    // Resource ID -> total capacity
    val capacityData = Map(
      "HOTEL101" -> 100,
      "FLIGHT101" -> 180,
      "BUS101" -> 40
    )

    val broadcastCapacity =
      ssc.sparkContext.broadcast(capacityData)

    // 3. Read real-time booking events from a TCP socket
    // Format:
    // eventId,timestamp,resourceId,bookingId,eventType,quantity
    // eventType = BOOK or CANCEL
    val input = ssc.socketTextStream("localhost", 9999)

    // 4. Parse and validate events
    val events = input.flatMap { line =>
      val p = line.split(",", -1)

      if (p.length == 6) {
        try {
          val event = BookingEvent(
            p(0).trim,
            p(1).trim,
            p(2).trim,
            p(3).trim,
            p(4).trim.toUpperCase,
            p(5).trim.toInt
          )

          val valid =
            event.eventId.nonEmpty &&
            event.timestamp.nonEmpty &&
            event.resourceId.nonEmpty &&
            event.bookingId.nonEmpty &&
            event.quantity > 0 &&
            Set("BOOK", "CANCEL").contains(event.eventType)

          if (valid) Some(event) else None

        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }.persist(StorageLevel.MEMORY_AND_DISK)

    // 5. Pair RDD: resource -> booking event count
    val eventCounts = events
      .map(event => (event.resourceId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(60),
        Seconds(5)
      )

    eventCounts.foreachRDD { rdd =>
      rdd.collect().foreach {
        case (resourceId, count) =>
          println(
            s"WINDOW EVENT COUNT resource=$resourceId " +
            s"eventsInLast60Seconds=$count"
          )
      }
    }

    // 6. Maintain booking state keyed by booking ID
    val bookingEvents = events.map { event =>
      (event.bookingId, event)
    }

    val bookingState = bookingEvents
      .updateStateByKey[BookingState](updateBookingState _)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // 7. Calculate current occupancy and availability
    bookingState.foreachRDD { rdd =>

      val activeBookings = rdd.filter {
        case (_, state) => state.active
      }

      val occupancy = activeBookings
        .map {
          case (_, state) =>
            (
              state.resourceId,
              (1L, state.quantity.toLong)
            )
        }
        .reduceByKey {
          case ((count1, quantity1), (count2, quantity2)) =>
            (count1 + count2, quantity1 + quantity2)
        }
        .collect()
        .toMap

      val capacities = broadcastCapacity.value

      val reportRows = capacities.toSeq.map {
        case (resourceId, capacity) =>

          val (bookingCount, occupiedUnits) =
            occupancy.getOrElse(resourceId, (0L, 0L))

          val available =
            capacity.toLong - occupiedUnits

          val occupancyPercent =
            if (capacity > 0) {
              occupiedUnits.toDouble * 100.0 / capacity
            } else {
              0.0
            }

          (
            resourceId,
            bookingCount,
            occupiedUnits,
            capacity,
            available,
            occupancyPercent
          )
      }

      // 8. Generate Spark SQL reporting output
      import spark.implicits._

      val report = spark.createDataset(reportRows).toDF(
        "resource_id",
        "active_bookings",
        "occupied_units",
        "capacity",
        "available_units",
        "occupancy_percent"
      )

      println("===== CURRENT BOOKING OCCUPANCY REPORT =====")
      report.orderBy($"resource_id").show(false)

      // 9. Detect overbooking
      report.filter(
        $"occupied_units" > $"capacity"
      ).show(false)
    }

    // 10. Display booking and cancellation events
    events.foreachRDD { rdd =>
      rdd.collect().foreach { event =>
        println(
          s"BOOKING EVENT id=${event.eventId} " +
          s"resource=${event.resourceId} " +
          s"booking=${event.bookingId} " +
          s"type=${event.eventType} " +
          s"quantity=${event.quantity}"
        )
      }
    }

    // 11. Start the streaming application
    ssc.start()
    ssc.awaitTermination()
  }
}
