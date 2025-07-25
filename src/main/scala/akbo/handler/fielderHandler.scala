package akbo.handler

import akbo.GlobalConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import sparkManager.sparkManager

object fielderHandler {
  def getKboFielderData(kboDSDir : String): Seq[DataFrame] = {
    val fielderDSDir : String = kboDSDir + GlobalConfig.FIELDER_DS_DIR_NAME
    //    val fielderDSDir : String = Paths.get(kboDSDir, GlobalConfig.FIELDER_DS_DIR_NAME).toString

    sparkManager.withSparkSession {
      (spark) => {
        val kboFielderYearlyDF = spark.read
          .parquet(fielderDSDir)
          .withColumn("tempIP", round(col("IP"), 3))
          .drop("IP")
          .withColumnRenamed("tempIP", "IP")

        val finalKboFielderYearlyDF = kboFielderYearlyDF
          .withColumn("FPCT", round((col("PO")+col("A"))/(col("PO")+col("A")+col("E")), 3))
          .withColumn("CS_per", round(col("CS")/(col("SB")+col("CS"))*100,1))
          .withColumn("data_type", lit("yearly"))
          .withColumn("player_id", col("id"))
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))


        Seq(finalKboFielderYearlyDF)
      }
    }
  }
}
