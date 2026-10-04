ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-day18-joins",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.9"
    )
  )
