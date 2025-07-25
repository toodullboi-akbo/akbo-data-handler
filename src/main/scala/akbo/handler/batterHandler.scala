package akbo.handler

import akbo.GlobalConfig
import exception.MyLittleException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import sparkManager.sparkManager

object batterHandler {
  def getKboBatterData(kboDSDir : String, whichTime : String, positionData : DataFrame): Seq[DataFrame] = {
    val batterDSDir: String = kboDSDir + GlobalConfig.BATTER_DS_DIR_NAME
    val batterYearlyDir : String = batterDSDir + "/" + GlobalConfig.YEARLY_DS_DIR_NAME
    val batterDailyDir : String = batterDSDir + "/" + GlobalConfig.BATTER_DAILY_DS_DIR_NAME
    val batterSitDir : String = batterDSDir +"/" + GlobalConfig.BATTER_SITUATION_DS_DIR_NAME
//    val batterLegDir : String = batterDSDir + "/" + GlobalConfig.LEGACY_DS_DIR_NAME

    val teamDSDir : String = kboDSDir + GlobalConfig.TEAM_DS_DIR_NAME
    val teamBatterDSDir : String = teamDSDir + "/batter"


    sparkManager.withSparkSession{
      (spark) => {

        val numbers : Option[String] = if (whichTime == "current") {
          Some(spark.read.csv(kboDSDir + "/" + "Entire_Batter_Number.csv")
            .select("_c0").rdd.map(_.getString(0)).collect().toSeq.tail.mkString("|"))
        } else {
          None
        }
        val kboBatterYearlyBasicOneDF = if( whichTime == "entire") {
          spark.read
            .parquet(batterYearlyDir+"/"+"basic_1")
        } else if (whichTime == "current"){
          spark.read
            .parquet(batterYearlyDir+"/"+"basic_1")
            .filter(input_file_name.contains(GlobalConfig.CURRENT_YEAR))
        } else {
          throw MyLittleException("whichTime is wrong !!!")
        }

        val kboBatterYearlyBasicTwoDF = if( whichTime == "entire") {
          spark.read
            .parquet(batterYearlyDir+"/"+"basic_2")
        } else if (whichTime == "current") {
          spark.read
            .parquet(batterYearlyDir+"/"+"basic_2")
            .filter(input_file_name.contains(GlobalConfig.CURRENT_YEAR))
        } else {
          throw MyLittleException("whichTime is wrong !!!")
        }

        val kboBatterYearlyDetailDF = if( whichTime == "entire") {
          spark.read
            .parquet(batterYearlyDir+"/"+"detail")
        } else if ( whichTime == "current" ){
          spark.read
            .parquet(batterYearlyDir+"/"+"detail")
            .filter(input_file_name.contains(GlobalConfig.CURRENT_YEAR))
        } else {
          throw MyLittleException("whichTime is wrong !!!")
        }

        val kboBatterDailyDF = if( whichTime == "entire") {
          spark.read
            .option("dateFormat","yyyy-MM-dd")
            .option("mergeSchema", "true")
            .parquet(batterDailyDir)
        } else if ( whichTime == "current" ){
          spark.read
            .option("dateFormat","yyyy-MM-dd")
            .parquet(batterDailyDir)
            .filter(input_file_name.rlike(numbers.get))
            .withColumn("year", year(col("date")))
            .filter(col("year") === s"${GlobalConfig.CURRENT_YEAR}")
            .drop(col("year"))
        } else {
          throw MyLittleException("whichTime is wrong !!!")
        }

        val kboBatterSitDF = if( whichTime == "entire") {
          spark.read
            .parquet(batterSitDir)
        } else if ( whichTime == "current" ){
          spark.read
            .parquet(batterSitDir)
            .filter(input_file_name.rlike(numbers.get))
            .filter(col("year") === s"${GlobalConfig.CURRENT_YEAR}")

        } else {
          throw MyLittleException("whichTime is wrong !!!")
        }

//        val kboBatterLegDF : Option[DataFrame] = if( whichTime == "entire") {
//          Some(spark.read
//            .parquet(batterLegDir))
//        } else {
//          None
//        }

        /////////////////////////////////
        // yearly
        /////////////////////////////////
        val kboBatterYearlyDF = kboBatterYearlyBasicOneDF
          .join(kboBatterYearlyBasicTwoDF, Seq("id", "year"), "left")
          .join(kboBatterYearlyDetailDF, Seq("id","year"), "left")

        // AVG // of daily game
        val windowSpec = Window.partitionBy("id","year")
          .orderBy(desc("date"))
        val seasonAVGDF = kboBatterDailyDF
          .withColumn("year", year(col("date")))
          .withColumn("rowNo",row_number.over(windowSpec))
          .where(col("rowNo") === 1)
          .select(col("id"), col("year"), col("seasonAVG").alias("AVG"))

        // G,PA,AB,R,H,2B,3B,HR,RBI,BB,HBP,SO,GDP // sum ( or counting ) of daily game
        val dailyGameDF = kboBatterDailyDF
          .withColumn("year", year(col("date")))
          .groupBy("id","year")
          .agg(
            count("*").alias("G"),
            sum("PA").alias("PA"),
            sum("AB").alias("AB"),
            sum("R").alias("R"),
            sum("H").alias("H"),
            sum("2B").alias("2B"),
            sum("3B").alias("3B"),
            sum("HR").alias("HR"),
            sum("RBI").alias("RBI"),
            sum("BB").alias("BB"),
            sum("HBP").alias("HBP"),
            sum("SO").alias("SO"),
            sum("GDP").alias("GDP"),

          )


        // yearly - join
        val revisedKboBatterYearlyDF = kboBatterYearlyDF
          .join(dailyGameDF,Seq("id", "year"), "left")
          .join(seasonAVGDF,Seq("id", "year"), "left")


        // IS_REGULATED
        val teamBatterGameDF = spark.read
          .parquet(teamBatterDSDir)
          .select(col("team_name").alias("team"), col("year"), col("team_G"))

        val regulatedKboBatterYearlyDF = revisedKboBatterYearlyDF
          .join(teamBatterGameDF, Seq("team", "year"), "left")
          .withColumn("is_regulated", when(floor(col("team_G")*3.1) <= col("PA"), "Y").otherwise("N"))
          .drop(col("team_G"))


        // TB, SLG, OBP, OPS, XBH, ISOP, GPA, GO/AO, BB/K
        val finalKboBatterYearlyDF = regulatedKboBatterYearlyDF
//          .withColumns(Map(
//            "TB" -> (col("H") + col("2B") + (col("3B") * 2) + (col("HR") * 3)),
//            "SLG" ->  round(col("TB") / col("AB"),3),
//            "OBP" -> round((col("H")+col("BB")+col("HBP"))/(col("AB")+col("BB")+col("HBP")+col("SF")),3),
//            "OPS" -> round(col("SLG")+col("OBP"),3),
//            "XBH" -> (col("2B")+col("3B")+col("HR")),
//            "ISOP" -> round(col("SLG")-col("AVG"),3),
//            "GPA" -> round((col("OBP")*1.8+col("SLG"))/4,3),
//            "GO/AO" -> round(col("GO")/col("AO"),2),
//            "BB/K"-> round(col("BB")/col("SO"),2),
//            "data_type" -> lit("yearly"),
//            "player_id" -> col("id"),
//            "id" -> concat_ws("_", col("player_id"), col("year"), col("data_type"))
//          ))
          .withColumn("TB", col("H") + col("2B") + (col("3B") * 2) + (col("HR") * 3))
          .withColumn("SLG", round(col("TB") / col("AB"),3))
          .withColumn("OBP", round((col("H")+col("BB")+col("HBP"))/(col("AB")+col("BB")+col("HBP")+col("SF")),3))
          .withColumn("OPS", round(col("SLG")+col("OBP"),3))
          .withColumn("XBH", col("2B")+col("3B")+col("HR"))
          .withColumn("ISOP", round(col("SLG")-col("AVG"),3))
          .withColumn("GPA", round((col("OBP")*1.8+col("SLG"))/4,3))
          .withColumn("GO/AO", round(col("GO")/col("AO"),2))
          .withColumn("BB/K",round(col("BB")/col("SO"),2))
          .withColumn("data_type",lit("yearly"))
          .withColumn("player_id", col("id"))
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))

        /////////////////////////////////
        // daily
        /////////////////////////////////
        // gameAVG - merge
        val finalKboBatterDailyDF = kboBatterDailyDF
//          .withColumns(Map(
//            "year" -> year(col("date")),
//            "gameAVG" -> round(col("H")/col("AB"),3),
//            "data_type" -> lit("daily"),
//            "player_id" -> col("id"),
//            "id" -> concat_ws("_", col("player_id"),col("date"),col("data_type"))
//          ))
          .withColumn("year", year(col("date")))
          .withColumn("gameAVG", round(col("H")/col("AB"),3))
          .withColumn("data_type", lit("daily"))
          .withColumn("player_id", col("id"))
          .withColumn("id", concat_ws("_", col("player_id"),col("date"),col("data_type")))


        /////////////////////////////////
        // situation
        /////////////////////////////////
        val finalKboBatterSitDF = kboBatterSitDF
          .withColumn("data_type", lit("situation"))
          .withColumn("player_id", col("id"))
          .withColumn("super_category",
            when(col("category").contains("-"), "볼카운트").otherwise(
                when(col("category").contains("회") || col("category").contains("연장"), "이닝").otherwise(
                  when(col("category").contains("투수"), "투수유형").otherwise(
                    when(col("category").contains("아웃"), "아웃카운트").otherwise(
                      when(col("category").contains("타자"), "타순").otherwise(
                        "주자상황"
                      )
                    )
                  )
                )
            )
          )
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("category"), col("data_type")))


        /////////////////////////////////
        // legacy
        /////////////////////////////////
//        val finalKboBatterLegDF = if (whichTime == "entire") {
//          kboBatterLegDF.get
//            .withColumn("data_type", lit("legacy"))
//            .withColumn("player_id", col("id"))
//            .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))
//        } else {
//          spark.emptyDataFrame
//        }


        /////////////////////////////////
        // consolidated_player
        /////////////////////////////////
        val kboBatterRegulated = finalKboBatterYearlyDF.select(col("player_id").cast("int"),col("team"),col("name"),col("year"),col("is_regulated"))
          .join(
            finalKboBatterDailyDF.select(col("player_id"),col("throwing"),col("hitting")).distinct,
            Seq("player_id"),
            "left"
          )
          .join(
            positionData,
            Seq("player_id","year"),
            "left"
          )
          .withColumn("data_type", lit("batter"))
          .withColumn("id", concat_ws("_", col("player_id"), col("data_type")))

        /////////////////////////////////
        // final
        /////////////////////////////////
        //        finalKboBatterYearlyDF.show
        //        finalKboBatterLegDF.show
        //        finalKboBatterDailyDF.show
        //        finalKboBatterSitDF.show
        if ( whichTime == "current"){
          Seq(kboBatterRegulated, finalKboBatterYearlyDF,finalKboBatterDailyDF,finalKboBatterSitDF)
        }
        else {
//          Seq(kboBatterRegulated, finalKboBatterYearlyDF,finalKboBatterDailyDF,finalKboBatterSitDF,finalKboBatterLegDF)
          Seq(kboBatterRegulated, finalKboBatterYearlyDF,finalKboBatterDailyDF,finalKboBatterSitDF)
        }
      }
    }
  }
}
