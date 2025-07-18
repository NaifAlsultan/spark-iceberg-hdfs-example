ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "client",
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client4" %% "core" % "4.0.9",
      "com.lihaoyi" %% "ujson" % "4.2.1"
    )
  )
