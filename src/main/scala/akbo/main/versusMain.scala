package akbo.main

import akbo.GlobalConfig
import akbo.handler.cosmosHandler.storeDataToCosmos
import exception.MyLittleException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import secret.secretEnv
import sparkManager.sparkManager


object versusMain {
  private def getKboVersusData(kboDSDir : String) : Seq[DataFrame] = {
    val versusDSDir : String = kboDSDir + GlobalConfig.VERSUS_DS_DIR_NAME
    sparkManager.withSparkSession{
      (spark) => {
        val kboVersusData = spark.read.parquet(versusDSDir)

        val kboVersusPitcher = kboVersusData
          .select(col("pitcher_id"), col("pitcher"), col("pitcher_team"), col("year"))
          .distinct()
          .withColumn("data_type", lit("player_pitcher"))
          .withColumn("id", concat_ws("_", col("pitcher_id"), col("year"), col("data_type")))

        val kboVersusBatter = kboVersusData
          .select(col("batter_id"), col("batter"), col("batter_team"), col("year"))
          .distinct()
          .withColumn("data_type", lit("player_batter"))
          .withColumn("id", concat_ws("_", col("batter_id"), col("year"), col("data_type")))




        val finalKboVersusData = kboVersusData
          .withColumn("data_type", lit("versus"))
          .withColumn("id", concat_ws("_", col("pitcher_id"), col("batter_id"), col("year"),col("data_type")))


        Seq(finalKboVersusData, kboVersusPitcher, kboVersusBatter)

      }
    }
  }
  def main(args:Array[String]) : Unit = {
    try {

      val kboDSDir : String = secretEnv.CURRENT_KBO_DS_DIR

      val kboVersusData = getKboVersusData(kboDSDir)
      kboVersusData.foreach(x => storeDataToCosmos(x, "versus"))


    } catch {
      case ex : MyLittleException => println(ex.getMessage)
    } finally {
      sparkManager.stopSpark
    }
  }


}
