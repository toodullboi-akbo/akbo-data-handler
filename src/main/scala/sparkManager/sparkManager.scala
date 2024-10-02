package sparkManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkManager {
  lazy val spark: SparkSession = SparkSession
    .builder
    .appName("KBOspark")
    .config("spark.master","local")
    .enableHiveSupport()
    .getOrCreate()

  def withSparkSession[T](f: SparkSession => T): T = {
    f(spark)
  }

  def stopSpark(): Unit = {
    spark.stop()
  }
}