package akbo.handler

import akbo.GlobalConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import sparkManager.sparkManager

object runnerHandler {
  def getKboRunnerData(kboDSDir : String): Seq[DataFrame] = {
    val runnerDSDir : String = kboDSDir + GlobalConfig.RUNNER_DS_DIR_NAME
    //    val runnerDSDir : String = Paths.get(kboDSDir, GlobalConfig.RUNNER_DS_DIR_NAME).toString

    sparkManager.withSparkSession {
      (spark) => {
        val kboRunnerYearlyDF = spark.read
          .parquet(runnerDSDir)

        val finalKboRunnerYearlyDF = kboRunnerYearlyDF
          .withColumn("SBA", col("SB")+col("CS"))
          .withColumn("SB_per", round(col("SB")/col("SBA")*100,1))
          .withColumn("data_type", lit("yearly"))
          .withColumn("player_id", col("id"))
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))


        Seq(finalKboRunnerYearlyDF)
      }
    }

  }
}
