package akbo.handler

import akbo.GlobalConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sparkManager.sparkManager

object pitcherHandler {

  def getKboPitcherData(kboDSDir : String, positionData : DataFrame): Seq[DataFrame] = {
    val pitcherDSDir : String = kboDSDir + GlobalConfig.PITCHER_DS_DIR_NAME
    val pitcherYearlyDir : String = pitcherDSDir + "/" + GlobalConfig.YEARLY_DS_DIR_NAME
    val pitcherDailyDir : String = pitcherDSDir + "/" + GlobalConfig.PITCHER_DAILY_DS_DIR_NAME
    val pitcherSitDir : String = pitcherDSDir + "/" + GlobalConfig.PITCHER_SITUATION_DS_DIR_NAME
//    val pitcherLegDir : String = pitcherDSDir + "/" + GlobalConfig.LEGACY_DS_DIR_NAME

    val teamDSDir : String = kboDSDir + GlobalConfig.TEAM_DS_DIR_NAME
    val teamPitcherDSDir : String = teamDSDir + "/pitcher"


    sparkManager.withSparkSession {
      (spark) => {

        val kboPitcherYearlyBasicTwoDF = spark.read
          .parquet(pitcherYearlyDir+"/"+"basic_2")
        val kboPitcherYearlyDetailOneDF = spark.read
          .parquet(pitcherYearlyDir+"/"+"detail_1")


        val kboPitcherDailyDF = spark.read
          .option("dateFormat","yyyy-MM-dd")
          .parquet(pitcherDailyDir)
          .withColumn("tempFloatIP", round(col("IP"),3))
          .drop("IP")
          .withColumnRenamed("tempFloatIP","IP")

        val kboPitcherSitDF = spark.read
          .parquet(pitcherSitDir)

//        val kboPitcherLegDF = spark.read
//          .parquet(pitcherLegDir)
        /////////////////////////////////
        // yearly
        /////////////////////////////////
        val kboPitcherYearlyDF = kboPitcherYearlyBasicTwoDF
          .join(kboPitcherYearlyDetailOneDF, Seq("id", "year"), "left")

        // ERA
        val windowSpec = Window.partitionBy("id","year")
          .orderBy(desc("date"))

        val seasonERADF = kboPitcherDailyDF
          .withColumn("year", year(col("date")))
          .withColumn("rowNo",row_number.over(windowSpec))
          .where(col("rowNo") === 1)
          .select(col("id"),col("year"),col("seasonERA").alias("ERA"))

        // G, tempIP, H, HR, BB, HBP, SO, R, ER
        val dailyGameDF = kboPitcherDailyDF
          .withColumn("year", year(col("date")))
          .groupBy("id","year")
          .agg(
            count("*").alias("G"),
            sum("IP").alias("tempIP"),
            sum("H").alias("H"),
            sum("HR").alias("HR"),
            sum("BB").alias("BB"),
            sum("HBP").alias("HBP"),
            sum("SO").alias("SO"),
            sum("R").alias("R"),
            sum("ER").alias("ER"),
            sum(when(col("res") === "승", 1).otherwise(0)).alias("W"),
            sum(when(col("res") === "패", 1).otherwise(0)).alias("L"),
            sum(when(col("res") === "세", 1).otherwise(0)).alias("SV"),
            sum(when(col("res") === "홀", 1).otherwise(0)).alias("HLD")
          )

//        // W
//        val dailyWinDF = kboPitcherDailyDF
//          .withColumn("year", year(col("date")))
//          .where(col("res") === "승")
//          .groupBy("id", "year")
//          .agg(
//            count("date").alias("W")
//          )
//
//        // L
//        val dailyLoseDF = kboPitcherDailyDF
//          .withColumn("year", year(col("date")))
//          .where(col("res") === "패")
//          .groupBy("id", "year")
//          .agg(
//            count("date").alias("L")
//          )
//
//        // SV
//        val dailySaveDF = kboPitcherDailyDF
//          .withColumn("year", year(col("date")))
//          .where(col("res") === "세")
//          .groupBy("id", "year")
//          .agg(
//            count("date").alias("SV")
//          )
//
//        // HOLD
//        val dailyHoldDF = kboPitcherDailyDF
//          .withColumn("year", year(col("date")))
//          .where(col("res") === "홀")
//          .groupBy("id", "year")
//          .agg(
//            count("date").alias("HLD")
//          )
//

        // yearly - join
        val revisedKboPitcherYearlyDF = kboPitcherYearlyDF
          .join(seasonERADF,Seq("id","year"), "left")
          .join(dailyGameDF,Seq("id","year"), "left")
//          .join(dailyWinDF,Seq("id","year"), "left")
//          .join(dailyLoseDF, Seq("id","year"), "left")
//          .join(dailySaveDF, Seq("id","year"), "left")
//          .join(dailyHoldDF, Seq("id","year"), "left")
          .na.fill(0)


        // WPCT, IP, WHIP, AVG, BABIP, P/G, P/IP, K/9, BB/9, K/BB, OBP, SLG, OPS
        val consolidatdKboPitcherYearlyDF = revisedKboPitcherYearlyDF
//          .withColumns(Map(
//            "WPCT" -> round(col("W")/(col("W")+col("L")),3),
//            "IP" -> when((col("tempIP") % 1) >= 0.9, floor(col("tempIP"))+1)
//              .otherwise(
//                floor(col("tempIP")) +
//                  when((col("tempIP") % 1) < 0.3, 0)
//                    .when((col("tempIP") % 1) < 0.6, 0.333)
//                    .otherwise(0.667)
//              ),
//            "WHIP" -> round((col("H")+col("BB"))/col("IP"),2),
//            "AVG" -> round(col("H")/(col("TBF")-col("BB")-col("HBP")-col("SF")-col("SAC")),3),
//            "BABIP" -> round((col("H")-col("HR"))/(col("TBF")-col("BB")-col("HBP")-col("SAC")-col("SO")-col("HR")),3),
//            "P/G" -> round(col("NP")/col("G"),1),
//            "P/IP" ->  round(col("NP")/col("IP"),1),
//            "K/9" -> round(col("SO")/col("IP")*9,2),
//            "BB/9" ->  round(col("BB")/col("IP")*9,2),
//            "K/BB" -> round(col("SO")/col("BB"),2),
//            "OBP" -> round((col("H")+col("BB")+col("HBP"))/(col("TBF")-col("SAC")),3),
//            "SLG" -> round((col("H") + col("2B") + (col("3B") * 2) + (col("HR") * 3))/(col("TBF")-col("BB")-col("HBP")-col("SF")-col("SAC")),3),
//            "OPS" -> round(col("OBP")+col("SLG"),3),
//            "data_type" -> lit("yearly"),
//            "player_id" -> col("id"),
//            "id" -> concat_ws("_", col("player_id"), col("year"), col("data_type"))
//          ))
//          .drop("tempIP")
//          .na.fill(0)

          .withColumn("WPCT", round(col("W")/(col("W")+col("L")),3))
          .withColumn("IP",
            when((col("tempIP") % 1) >= 0.9, floor(col("tempIP"))+1)
              .otherwise(
                floor(col("tempIP")) +
                  when((col("tempIP") % 1) < 0.3, 0)
                    .when((col("tempIP") % 1) < 0.6, 0.333)
                    .otherwise(0.667)
              )
          )
          .drop("tempIP")
          .withColumn("WHIP",round((col("H")+col("BB"))/col("IP"),2))
          .withColumn("AVG", round(col("H")/(col("TBF")-col("BB")-col("HBP")-col("SF")-col("SAC")),3))
          .withColumn("BABIP", round((col("H")-col("HR"))/(col("TBF")-col("BB")-col("HBP")-col("SAC")-col("SO")-col("HR")),3))
          .withColumn("P/G", round(col("NP")/col("G"),1))
          .withColumn("P/IP", round(col("NP")/col("IP"),1))
          .withColumn("K/9", round(col("SO")/col("IP")*9,2))
          .withColumn("BB/9", round(col("BB")/col("IP")*9,2))
          .withColumn("K/BB",round(col("SO")/col("BB"),2))
          .withColumn("OBP",round((col("H")+col("BB")+col("HBP"))/(col("TBF")-col("SAC")),3))
          .withColumn("SLG",round((col("H") + col("2B") + (col("3B") * 2) + (col("HR") * 3))/(col("TBF")-col("BB")-col("HBP")-col("SF")-col("SAC")),3))
          .withColumn("OPS",round(col("OBP")+col("SLG"),3))
          .na.fill(0)
          .withColumn("data_type", lit("yearly"))
          .withColumn("player_id", col("id"))
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))


        // IS_REGULATED
        val teamPitcherGameDF = spark.read
          .parquet(teamPitcherDSDir)
          .select(col("team_name").alias("team"), col("year"), col("team_G"))

        val finalKboPitcherYearlyDF = consolidatdKboPitcherYearlyDF
          .join(teamPitcherGameDF, Seq("team", "year"), "left")
          .withColumn("is_regulated", when(col("team_G") <= col("IP"), "Y").otherwise("N"))
          .drop(col("team_G"))

        /////////////////////////////////
        // daily
        /////////////////////////////////
        val finalKboPitcherDailyDF = kboPitcherDailyDF
          .withColumn("year", year(col("date")))
          .withColumn("data_type", lit("daily"))
          .withColumn("player_id", col("id"))
          .withColumn("id", concat_ws("_", col("player_id"), col("date"), col("data_type")))


        /////////////////////////////////
        // situation
        /////////////////////////////////
        val finalKboPitcherSitDF = kboPitcherSitDF
          .withColumn("data_type", lit("situation"))
          .withColumn("player_id", col("id"))
          .withColumn("super_category",
            when(col("category").contains("-"), "볼카운트").otherwise(
              when(col("category").contains("회") || col("category").contains("연장"), "이닝").otherwise(
                when(col("category").contains("아웃"), "아웃카운트").otherwise(
                    when(col("category").contains("좌타자") || col("category").contains("우타자"), "타자유형").otherwise(
                  when(col("category").contains("번"), "타순")
                    .otherwise("주자상황")
                  )
                )
              )
            )
          )
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("category"), col("data_type")))

        /////////////////////////////////
        // legacy
        /////////////////////////////////
//        val finalKboPitcherLegDF = kboPitcherLegDF
//          .withColumn("data_type", lit("legacy"))
//          .withColumn("player_id", col("id"))
//          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))

        /////////////////////////////////
        // consolidated_player
        /////////////////////////////////
        val kboPitcherRegulated = finalKboPitcherYearlyDF.select(col("player_id").cast("int"),col("team"),col("name"),col("year"),col("is_regulated"))
          .join(
            finalKboPitcherDailyDF.select(col("player_id"),col("throwing"),col("hitting")).distinct,
            Seq("player_id"),
            "left"
          )
          .join(
            positionData,
            Seq("player_id","year"),
            "left"
          )
          .withColumn("data_type", lit("pitcher"))
          .withColumn("id", concat_ws("_", col("player_id"), col("data_type")))



        /////////////////////////////////
        // final
        /////////////////////////////////
        //        finalKboPitcherYearlyDF.show
        //        finalKboPitcherDailyDF.show
        //        finalKboPitcherSitDF.show
        //        finalKboPitcherLegDF.show

//        Seq(kboPitcherRegulated, finalKboPitcherYearlyDF, finalKboPitcherDailyDF, finalKboPitcherSitDF, finalKboPitcherLegDF)
          Seq(kboPitcherRegulated, finalKboPitcherYearlyDF, finalKboPitcherDailyDF, finalKboPitcherSitDF)

      }
    }
  }
}
