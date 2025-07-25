ThisBuild / version := "2.0.0"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "kbo-data-handler"
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

  "com.azure.cosmos.spark" % "azure-cosmos-spark_3-5_2-12" % "4.36.0"
)