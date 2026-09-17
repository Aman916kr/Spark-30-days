ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "SparkDay4",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.9",
      "org.apache.spark" %% "spark-sql" % "3.5.9"
    )
  )
