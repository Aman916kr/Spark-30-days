ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Day11-Broadcast-Accumulators"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.9"
)
