# Day 28 — Real-Time Booking Monitoring with Apache Spark

## Overview

This project implements a real-time booking monitoring system using Apache Spark Streaming and Scala. It processes hotel, flight, and bus booking events, maintains booking and cancellation state, calculates occupancy and availability, aggregates event counts using sliding windows, and generates reports using Spark SQL.

## Objectives

* Process real-time hotel, flight, and bus booking events.
* Maintain booking and cancellation state.
* Calculate active bookings, occupancy, and available capacity.
* Use Pair RDDs and sliding-window operations.
* Broadcast resource capacity reference data.
* Generate tabular reports using Spark SQL.
* Practice micro-batch processing and stateful streaming.

## Technologies

* Apache Spark 3.5.9
* Scala 2.12.18
* SBT 1.11.7
* Spark Streaming
* Spark SQL
* Netcat
* Ubuntu/WSL
* VS Code

## Project Structure

```
spark-day28/
├── build.sbt
├── README.md
├── checkpoint/
└── src/
    └── main/
        └── scala/
            └── BookingMonitoring.scala
```

## Event Schema

Each input event follows this format:

```
eventId,timestamp,resourceId,bookingId,eventType,quantity
```

| Field      | Description                      |
| ---------- | -------------------------------- |
| eventId    | Unique event identifier          |
| timestamp  | Event timestamp                  |
| resourceId | Hotel, flight, or bus identifier |
| bookingId  | Booking identifier               |
| eventType  | BOOK or CANCEL                   |
| quantity   | Number of units booked           |

## Reference Data

| Resource  | Capacity |
| --------- | -------: |
| HOTEL101  |      100 |
| FLIGHT101 |      180 |
| BUS101    |       40 |

These capacities are sample values for the lab.

## Processing Logic

### 1. Event Validation

The application checks the number of fields, required identifiers, event type, and positive quantity. Invalid records are discarded.

### 2. Booking State

Booking events are keyed by booking ID and processed using `updateStateByKey`.

* BOOK creates an active booking.
* CANCEL marks an active booking as cancelled.
* Duplicate bookings while active are ignored.
* Invalid cancellations are ignored.

### 3. Window Aggregation

A 60-second sliding window updated every five seconds counts booking and cancellation events by resource. This represents event volume, not occupancy.

### 4. Occupancy and Availability

Active bookings are grouped by resource. The report calculates:

* Active booking count.
* Occupied units.
* Total capacity.
* Available units.
* Occupancy percentage.

Available units are calculated as capacity minus occupied units.

### 5. Broadcast Reference Data

The resource capacity map is broadcast to Spark workers to avoid repeatedly distributing the reference data.

### 6. Spark SQL Reporting

Each micro-batch produces a DataFrame showing current occupancy and availability for the configured resources.

## How to Run

### Compile

```
sbt compile
```

### Start Netcat

In Terminal 1:

```
nc -lk 9999
```

### Start the Application

In Terminal 2:

```
cd ~/spark-30-days/spark-day28
sbt run
```

### Send Sample Events

```
EVT001,2026-10-13T10:00:00,HOTEL101,BOOK001,BOOK,2
EVT002,2026-10-13T10:00:05,HOTEL101,BOOK002,BOOK,3
EVT003,2026-10-13T10:00:10,HOTEL101,BOOK003,BOOK,1
EVT004,2026-10-13T10:00:15,HOTEL101,BOOK002,CANCEL,3
EVT005,2026-10-13T10:00:20,FLIGHT101,BOOK004,BOOK,4
EVT006,2026-10-13T10:00:25,BUS101,BOOK005,BOOK,5
```

## Expected Results

The application displays booking events, resource event counts for the sliding window, and a Spark SQL report containing active bookings, occupied units, capacity, available units, and occupancy percentage.

## Important Limitations

* The TCP socket is a learning input source, not a durable event broker.
* Booking state is maintained in Spark Streaming state and requires checkpointing.
* The sample implementation assumes booking IDs identify individual bookings.
* State and occupancy are not stored in an external database.
* Capacity values are illustrative.
* Production use would require durable input, stronger event deduplication, state recovery testing, and business rules for partial cancellations and capacity changes.


