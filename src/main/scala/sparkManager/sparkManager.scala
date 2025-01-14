package sparkManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import secret.secretEnv

object sparkManager {
  private val logDirectory = s"wasbs://${secretEnv.AZURE_BLOB_SPARK_LOG_CONTAINER_NAME}@${secretEnv.AZURE_BLOB_STORAGE_ACCOUNT}.blob.core.windows.net/logs/"

  lazy val spark = SparkSession.builder()
    .appName("KBOspark")
    .master("local")
    .config(s"fs.azure.account.key.${secretEnv.AZURE_BLOB_STORAGE_ACCOUNT}.blob.core.windows.net", secretEnv.AZURE_BLOB_STORAGE_ACCOUNT_KEY)
    .config(s"spark.hadoop.fs.azure.account.key.${secretEnv.AZURE_BLOB_STORAGE_ACCOUNT}.blob.core.windows.net", secretEnv.AZURE_BLOB_STORAGE_ACCOUNT_KEY)
    .config("spark.eventLog.enabled", "true") // Enable event logging
    .config("spark.eventLog.dir", logDirectory) // Set Azure Blob Storage as log directory
    .config("spark.history.fs.logDirectory", logDirectory) // Set the history server log directory
    .getOrCreate()

  def withSparkSession[T](f: SparkSession => T): T = {
    f(spark)
  }

  def stopSpark(): Unit = {
    spark.stop()
  }
}