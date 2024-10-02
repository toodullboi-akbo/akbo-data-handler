ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "scala-dustmq"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.2",
  "org.apache.spark" %% "spark-sql" % "3.5.2",
  "org.apache.spark" %% "spark-hive" % "3.5.2"
)