# Day 21 — Spark Catalog

## Objective
Learn how to use Spark Catalog to list databases and tables, create temporary views, register tables, query data using Spark SQL, and inspect table schema and metadata.

## Concepts Covered
- Spark Catalog
- Database creation and selection
- Temporary views
- Table registration using `saveAsTable()`
- Listing databases and tables
- Schema inspection using `DESCRIBE`
- Table metadata using `DESCRIBE EXTENDED`
- SQL queries on registered tables

## Project Structure
```text
spark-day21/
├── build.sbt
├── data/
│   └── hotel_bookings.csv
└── src/
    └── main/
        └── scala/
            └── HotelCatalogAnalysis.scala
```

## Dataset
The `hotel_bookings.csv` dataset contains 10 hotel booking records with the following columns:

- `booking_id`
- `guest_name`
- `hotel_name`
- `city`
- `room_type`
- `check_in`
- `check_out`
- `amount`
- `status`

## Implementation

### 1. Read the Dataset
Load hotel bookings into a Spark DataFrame.

### 2. Create a Temporary View
Register the DataFrame as a temporary SQL view.

```scala
bookings.createOrReplaceTempView("hotel_bookings")
```

Query the view to calculate booking counts and revenue by hotel.

```sql
SELECT hotel_name,
       COUNT(*) AS total_bookings,
       SUM(amount) AS total_revenue
FROM hotel_bookings
GROUP BY hotel_name
ORDER BY hotel_name;
```

### 3. Create a Database
Create and select the analytics database.

```sql
CREATE DATABASE IF NOT EXISTS hotel_analytics;
USE hotel_analytics;
```

### 4. Register a Table
Save the DataFrame as a catalog table.

```scala
bookings.write
  .mode("overwrite")
  .saveAsTable("bookings")
```

### 5. List Databases and Tables
Use Spark SQL to inspect available databases and tables.

```sql
SHOW DATABASES;
SHOW TABLES;
```

### 6. Inspect Schema and Metadata
Inspect column names and data types.

```sql
DESCRIBE bookings;
```

Inspect additional table metadata, including provider and storage location.

```sql
DESCRIBE EXTENDED bookings;
```

### 7. Query the Registered Table
Calculate booking counts and revenue by city.

```sql
SELECT city,
       COUNT(*) AS total_bookings,
       SUM(amount) AS total_revenue
FROM bookings
GROUP BY city
ORDER BY city;
```

## Expected Output

### 1. Booking Summary by Hotel

```text
+------------+--------------+-------------+
|hotel_name  |total_bookings|total_revenue|
+------------+--------------+-------------+
|Grand Palace|4             |59000        |
|Lake View   |3             |43000        |
|Royal Inn   |3             |46000        |
+------------+--------------+-------------+
```

### 2. Databases

The database listing should include:

```text
hotel_analytics
```

Other databases, including Spark's default database, may also appear.

### 3. Tables

The table listing for `hotel_analytics` should include:

```text
bookings
```

### 4. Schema Inspection

`DESCRIBE bookings` should display columns such as:

```text
booking_id
guest_name
hotel_name
city
room_type
check_in
check_out
amount
status
```

The inferred data types depend on the CSV contents and Spark's schema inference.

### 5. Revenue Summary by City

```text
+---------+--------------+-------------+
|city     |total_bookings|total_revenue|
+---------+--------------+-------------+
|Bengaluru|3             |43000        |
|Hyderabad|4             |59000        |
|Mumbai   |3             |46000        |
+---------+--------------+-------------+
```

### 6. Table Metadata
`DESCRIBE EXTENDED bookings` should provide additional metadata, such as table type, provider, and location. Exact values depend on the active Spark catalog and warehouse configuration.

## How to Run

Open the Ubuntu terminal and navigate to the project:

```bash
cd ~/spark-30-days/spark-day21
```

Run the application:

```bash
sbt run
```

