package akbo.handler

import akbo.GlobalConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import sparkManager.sparkManager

object teamHandler {
  def getTeamData(kboDSDir : String) : Seq[DataFrame] = {
    val teamDSDir : String = kboDSDir + GlobalConfig.TEAM_DS_DIR_NAME
    val teamBatterDSDir : String = teamDSDir + "/batter"
    val teamPitcherDSDir : String = teamDSDir + "/pitcher"
    val teamFielderDSDir : String = teamDSDir + "/fielder"
    val teamRunnerDSDir : String = teamDSDir + "/runner"

    sparkManager.withSparkSession{
      (spark) => {
        val kboTeamBatterDF = spark.read.parquet(teamBatterDSDir)
        val kboTeamPitcherDF = spark.read.parquet(teamPitcherDSDir)
        val kboTeamFielderDF = spark.read.parquet(teamFielderDSDir)
        val kboTeamRunnerDF = spark.read.parquet(teamRunnerDSDir)

        ///////////////

        val finalKboTeamBatterDF = kboTeamBatterDF
          .withColumn("data_type", lit("batter"))
          .withColumn("id", concat_ws("_", col("team_name"), col("year"), col("data_type")))

        val finalKboTeamPitcherDF = kboTeamPitcherDF
          .withColumn("data_type", lit("pitcher"))
          .withColumn("id", concat_ws("_", col("team_name"), col("year"), col("data_type")))

        val finalKboTeamFielderDF = kboTeamFielderDF
          .withColumn("data_type", lit("fielder"))
          .withColumn("id", concat_ws("_", col("team_name"), col("year"), col("data_type")))

        val finalKboTeamRunnerDF = kboTeamRunnerDF
          .withColumn("data_type", lit("runner"))
          .withColumn("id", concat_ws("_", col("team_name"), col("year"), col("data_type")))


        Seq(finalKboTeamBatterDF, finalKboTeamPitcherDF, finalKboTeamFielderDF, finalKboTeamRunnerDF)
      }
    }

  }
}
