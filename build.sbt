ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "scala-dustmq"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.2",
  "org.apache.spark" %% "spark-sql" % "3.5.2",
  "org.apache.spark" %% "spark-hive" % "3.5.2",

  "com.azure" % "azure-storage-blob" % "12.25.0",

  "com.azure" % "azure-identity" % "1.11.2",
  "com.microsoft.azure" % "azure-storage" % "8.6.6",

  "org.apache.hadoop" % "hadoop-azure" % "3.3.6",
  "org.apache.hadoop" % "hadoop-azure-datalake" % "3.3.6",
  "org.apache.hadoop" % "hadoop-common" % "3.3.6", // Add this explicitly

  "org.apache.hadoop" % "hadoop-client" % "3.3.6",
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.6",
  "org.apache.hadoop" % "hadoop-client-runtime" % "3.3.6",
)