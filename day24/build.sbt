ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-day24-stateful-streaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % "3.5.9"
    )
  )
