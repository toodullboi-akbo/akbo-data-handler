package sparkManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import secret.secretEnv

object sparkManager {

  lazy val spark = SparkSession.builder()
    .appName("KBOspark")
//    .config("spark.master", "local")
    .config(s"fs.azure.account.key.${secretEnv.AZURE_BLOB_STORAGE_ACCOUNT}.blob.core.windows.net",secretEnv.AZURE_BLOB_STORAGE_ACCOUNT_KEY)
    .getOrCreate()


  def withSparkSession[T](f: SparkSession => T): T = {
    f(spark)
  }

  def stopSpark(): Unit = {
    spark.stop()
  }
}