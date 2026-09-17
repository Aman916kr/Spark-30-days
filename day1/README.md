# StudentGrade

A beginner-friendly Scala project that demonstrates Scala Essentials by building a simple Student Grade Processor.

## Technologies Used

- Scala 2.12.18
- SBT 1.11.7
- Ubuntu Linux

## Project Structure

```text
StudentGrade/
├── build.sbt
├── README.md
└── src/
    └── main/
        └── scala/
            └── StudentGrade.scala
```
## How to Run
```
Open Ubuntu Terminal and run:

cd ~/StudentGrade
sbt run

The project will compile and execute the StudentGrade Scala program.
```
## Output
```
====================================
        STUDENT GRADE
     Student Grade Processor
====================================

--- VAL ---
College: ABC College

--- VAR ---
Initial student count: 3
Updated student count: 5

--- LAZY VAL ---
Before accessing lazy value
Lazy value is calculated now
Student Grade Processor Started
After accessing lazy value

--- LIST ---
List(Aman, Rahul, Priya)
First student: Aman

--- VECTOR ---
Vector(Aman, Rahul, Priya)
Second student: Rahul

--- SET ---
Set(Scala, Spark, Kafka)

--- MAP ---
Map(Aman -> 85, Rahul -> 72, Priya -> 91)
Aman's marks: 85

--- IMMUTABLE COLLECTION ---
Original: List(Aman, Rahul, Priya)
New: List(Aman, Rahul, Priya, Vikram)

--- FOR COMPREHENSION ---
Aman scored 85
Aman scored 72
Aman scored 91
Rahul scored 85
Rahul scored 72

--- TRAIT LOGGER ---
[LOG] Processing student: Aman, Mark: 85
[LOG] Aman received Grade B

--- STUDENT GRADE PROCESSOR ---
Aman -> Marks: 85 -> Grade: B
Rahul -> Marks: 72 -> Grade: C
Priya -> Marks: 91 -> Grade: A

====================================    
       STUDENT GRADE COMPLETE
====================================
