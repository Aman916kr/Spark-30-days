# Day 26 — Real-Time Healthcare Monitoring with Apache Spark Streaming

## 📌 Overview

This project implements a real-time healthcare monitoring system using **Apache Spark Streaming and Scala**. It receives patient vital readings through a TCP socket, validates the input, identifies abnormal measurements, generates alerts, and detects repeated abnormal readings within a time window.

The project demonstrates Spark Streaming micro-batches, broadcast variables, accumulators, window-based aggregation, checkpointing, partitions, DAG execution, and fault tolerance.

## 🎯 Objectives

- Design a patient-vital event schema.
- Process real-time patient vital readings.
- Detect abnormal heart rate, low oxygen saturation, and high temperature.
- Generate individual alerts for abnormal readings.
- Detect repeated abnormal readings within a 60-second window.
- Use broadcast variables to distribute threshold configurations.
- Use accumulators to track abnormal readings.
- Understand Spark partitions, DAG execution, and checkpointing.

## 🛠️ Technology Stack

- **Language:** Scala 2.12.18
- **Processing Engine:** Apache Spark 3.5.9
- **Streaming API:** Spark Streaming (DStreams)
- **Build Tool:** SBT 1.11.7
- **Java:** Java 11
- **Input Source:** TCP socket using Netcat
- **Execution Environment:** Ubuntu on WSL

## 📂 Project Structure

```text
spark-day26/
├── build.sbt
├── checkpoint/
└── src/
    └── main/
        └── scala/
            └── HealthcareMonitoring.scala
```

## 🧾 Patient-Vital Event Schema

Each input event contains five comma-separated fields:

| Field | Data Type | Description |
|---|---|---|
| patientId | String | Unique patient identifier |
| timestamp | String | Time of the reading |
| heartRate | Double | Heart rate in beats per minute |
| spo2 | Double | Blood oxygen saturation percentage |
| temperature | Double | Body temperature in Celsius |

Example event:

```text
P101,2026-10-12T13:00:00,75,98,36.8
```

The application validates the field count, numeric values, patient ID, timestamp, and permitted value ranges before processing a reading.

## 🚨 Abnormal-Vital Thresholds

The application uses a broadcast variable to distribute the threshold configuration.

| Vital | Condition | Alert |
|---|---|---|
| Heart rate | Below 50 or above 120 | ABNORMAL_HEART_RATE |
| SpO₂ | Below 92% | LOW_SPO2 |
| Temperature | Above 38°C | HIGH_TEMPERATURE |

**Note:** These thresholds are illustrative values for a learning exercise, not clinical guidance or a validated medical monitoring protocol.

## ⚙️ Implementation

### 1. Configure the project

The `build.sbt` file defines the Scala version and Spark Streaming dependency.

```scala
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-day26-healthcare-monitoring",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.9"
    )
  )
```

### 2. Create the streaming context

The application creates a Spark Streaming context with a five-second batch interval.

```scala
val ssc = new StreamingContext(conf, Seconds(5))
ssc.checkpoint("checkpoint")
```

Incoming events are processed in micro-batches every five seconds.

### 3. Receive and validate patient data

The application receives events from `localhost:9999` using `socketTextStream`. Each event is split into fields, parsed, and validated before being passed to the abnormal-vital detection logic.

### 4. Detect abnormal vital readings

The application compares each reading against the broadcast thresholds. If one or more conditions are abnormal, it creates an alert containing the patient ID, timestamp, and alert reasons.

Example output:

```text
ALERT patient=P103 time=2026-10-12T13:00:10 reason=ABNORMAL_HEART_RATE|LOW_SPO2|HIGH_TEMPERATURE
```

The application generates an individual alert for each abnormal reading.

### 5. Detect repeated abnormal readings

The application maps abnormal readings to patient IDs, counts them using `reduceByKeyAndWindow`, and uses a 60-second window with a five-second slide interval.

When a patient has at least three abnormal readings in the window, the application prints a repeated-alert message.

Example output:

```text
REPEATED ABNORMAL READINGS patient=P103 count=3
```

### 6. Use a broadcast variable

The threshold map is broadcast to Spark executors. This avoids repeatedly distributing the full threshold configuration with each task.

### 7. Use an accumulator

A Spark `LongAccumulator` tracks abnormal-reading events for demonstration and monitoring.

Accumulators are useful for counters, but task retries can affect their observed values. They should not be treated as a guaranteed exactly-once alert count.

## ▶️ How to Run the Project

### Step 1: Open the project directory

```bash
cd ~/spark-30-days/spark-day26
```

### Step 2: Start the TCP input server

Open a separate Ubuntu terminal and run:

```bash
nc -lk 9999
```

Keep this terminal open to send patient events.

### Step 3: Compile the project

In the project terminal, run:

```bash
sbt compile
```

### Step 4: Start Spark Streaming

```bash
sbt run
```

Keep the Spark application running while entering events in the Netcat terminal.

### Step 5: Send normal readings

Enter the following lines one at a time:

```text
P101,2026-10-12T13:00:00,75,98,36.8
P102,2026-10-12T13:00:05,82,97,37.0
```

These readings should not generate abnormal-vital alerts under the configured thresholds.

### Step 6: Send abnormal readings

Enter the following lines one at a time:

```text
P103,2026-10-12T13:00:10,130,89,38.5
P103,2026-10-12T13:00:15,135,88,38.6
P103,2026-10-12T13:00:20,128,90,38.4
```

Wait for the Spark Streaming batches to process the readings.

## 📊 Expected Results

Individual alerts should identify the abnormal vital measurements. After three abnormal readings for the same patient fall within the 60-second window, the application should also produce output similar to:

```text
REPEATED ABNORMAL READINGS patient=P103 count=3
```

The repeated-alert message confirms that window-based aggregation is working for the test data.

## 🧠 Key Spark Concepts Learned

### Micro-batch processing
Spark Streaming groups incoming data into small batches and processes each batch at the configured interval.

### Transformations
Operations such as `flatMap`, `map`, `filter`, and `reduceByKeyAndWindow` transform and aggregate the incoming stream.

### Partitions and parallelism
Spark divides RDDs into partitions so that tasks can process separate portions of the data in parallel.

### DAG execution
Spark represents dependencies between transformations as a directed acyclic graph (DAG). Actions such as `collect()` trigger jobs that Spark executes through stages and tasks.

### Broadcast variables
Broadcast variables efficiently distribute read-only configuration data, such as the vital thresholds, to executors.

### Accumulators
Accumulators provide a way to track aggregate values, such as the number of abnormal-reading events, during distributed processing.

### Checkpointing and fault tolerance
Checkpointing stores streaming metadata and state-related information needed for recovery. RDD lineage also enables Spark to recompute lost partitions. Reliable recovery depends on appropriate checkpointing and source configuration.

## ⚠️ Limitations

- TCP socket input is intended for local testing, not production ingestion.
- The application uses illustrative thresholds rather than clinically validated limits.
- Checkpointing alone does not guarantee end-to-end exactly-once alert delivery.
- The accumulator is intended for learning and monitoring, not as an authoritative alert ledger.
- Production healthcare monitoring would require durable ingestion, secure transport, access controls, data privacy protections, monitoring, and validated clinical requirements.

## ✅ Final Outcome

The Day 26 project demonstrates a working real-time healthcare monitoring pipeline using Scala and Apache Spark Streaming. It validates incoming patient vital readings, detects abnormal measurements, emits individual alerts, identifies repeated abnormal readings using window-based aggregation, and demonstrates broadcast variables, accumulators, checkpointing, partitions, DAG execution, and fault-tolerance concepts.
