ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-day26-healthcare-monitoring",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.9"
    )
  )
