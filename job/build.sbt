ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"

val sparkVersion = "3.5.4"

lazy val root = (project in file("."))
  .settings(
    name := "job",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
    )
  )
