package akbo

import exception.MyLittleException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import secret.secretEnv
import sparkManager.sparkManager
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.input_file_name

object dataHandler{

  def blogsDF(spark : SparkSession, file : String ): Unit = {
    val jsonFile = file
    val schema = " Id INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>"

    val blogsDF = spark.read.schema(schema).json(jsonFile)

    blogsDF.show()

    blogsDF
      .withColumn("AuthorsID", (concat(col("First"), col("Last"), col("Id"))))
      .select(col("AuthorsID"))
      .show(4)
  }



  def callDF(spark : SparkSession, file : String) : Unit = {
    val callDF = spark.read.option("header","true").csv(file)

    callDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10,truncate=false)
  }

  private def getCurrentDF(kboDSDir : String) : Unit = {
    val batterDSDir: String = kboDSDir + GlobalConfig.BATTER_DS_DIR_NAME
    val batterYearlyDir : String = batterDSDir + "/" + GlobalConfig.YEARLY_DS_DIR_NAME
    val batterDailyDir : String = batterDSDir + "/" + GlobalConfig.BATTER_DAILY_DS_DIR_NAME
    val batterSitDir : String = batterDSDir +"/" + GlobalConfig.BATTER_SITUATION_DS_DIR_NAME
    val kboBatterYearlyBasicOneDir : String = batterYearlyDir + "/" + "basic_1"
    sparkManager.withSparkSession{
      (spark) => {
//        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//        val files = fs.listStatus(new Path(kboBatterYearlyBasicOneDir))
//          .map(_.getPath.toString)
//          .filter(_.contains("2024"))

//
//        kboBatterYearlyBasicOneDF.show

//        val kboBatterDailyDF = spark
//          .read.parquet(batterDailyDir)
//          .filter(input_file_name.contains("71432"))
//
//        kboBatterDailyDF.show
        val numberDF = spark.read.csv(kboDSDir + "/" + "Entire_Batter_Number.csv")
        val numbers : String =numberDF.select("_c0")
          .rdd.map(_.getString(0)).collect().toSeq.tail.mkString("|")
//          collect().toSeq
        print(numbers)


//        val pattern = Seq("2024","2023").map(_.toString).mkString("|")
//        val kboBatterYearlyBasicOneDF = spark
//          .read.parquet(kboBatterYearlyBasicOneDir)
//          .filter(input_file_name.rlike(pattern))
//          .filter(col("year") === "2022")
//          .show
//


        // yearly


      }
    }
  }
  private def storeDataToCosmos(df : DataFrame, containerName : String): Unit = {
    sparkManager.withSparkSession{
      (spark) =>{
        val dataType : String = df.select("data_type").first().getAs[String]("data_type")

        // making DB
        spark.sql(s"CREATE DATABASE IF NOT EXISTS cosmosCatalog.${secretEnv.AZURE_COSMOS_DB_NAME}")

        // making CONTAINER ( TABLE )
        spark.sql(
          s"""
             |CREATE TABLE IF NOT EXISTS cosmosCatalog.${secretEnv.AZURE_COSMOS_DB_NAME}.${containerName}
             |USING cosmos.oltp
             |TBLPROPERTIES (
             |  'partitionKeyPath' = '/year',
             |  'manualThroughput' = '1000'
             |)
             |""".stripMargin
        )

        // check existence
        val rawExistingDF = spark.read
          .format("cosmos.oltp")
          .options(secretEnv.AZURE_COSMOS_CONFIG
            + ("spark.cosmos.container" -> containerName)
          )
          .load()

        val existingDF =
          if (!rawExistingDF.isEmpty) rawExistingDF.filter(col("data_type") === dataType)
          else rawExistingDF

        if (existingDF.isEmpty){
          // first inserting
          df.write
            .format("cosmos.oltp")
            .options(
              secretEnv.AZURE_COSMOS_CONFIG
                + ("spark.cosmos.container" -> containerName)
            )
            .mode("APPEND")
            .save()

        } else {
          // field-level compare
          val fieldToCompare = df.columns
          val changedCondition = fieldToCompare.map(field => col(s"new.${field}") =!= col(s"old.${field}"))
          val joinedDF = df.alias("new")
            .join(existingDF.alias("old"), Seq("id", "year"), "inner")
          val changedDF = joinedDF.filter(changedCondition.reduce(_ || _))
            .select("new.*")

          // records-level compare
          val newRecordsDF = df.alias("new")
            .join(existingDF.alias("old"),
              (col("new.id") === col("old.id")) && (col("new.year") === col("old.year")),
              "leftanti"
            )


          val finalUpdateDF = newRecordsDF.union(changedDF)

          if (!finalUpdateDF.isEmpty) {
            finalUpdateDF.write
              .format("cosmos.oltp")
              .options(
                secretEnv.AZURE_COSMOS_CONFIG
                  + ("spark.cosmos.container" -> containerName)
              )
              .mode("APPEND")
              .save()
          }
        }
      }
    }
  }



  def getKboBatterData(kboDSDir : String, whichTime : String): Seq[DataFrame] = {
    val batterDSDir: String = kboDSDir + GlobalConfig.BATTER_DS_DIR_NAME
    val batterYearlyDir : String = batterDSDir + "/" + GlobalConfig.YEARLY_DS_DIR_NAME
    val batterDailyDir : String = batterDSDir + "/" + GlobalConfig.BATTER_DAILY_DS_DIR_NAME
    val batterSitDir : String = batterDSDir +"/" + GlobalConfig.BATTER_SITUATION_DS_DIR_NAME
    val batterLegDir : String = batterDSDir + "/" + GlobalConfig.LEGACY_DS_DIR_NAME

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

        val kboBatterLegDF : Option[DataFrame] = if( whichTime == "entire") {
          Some(spark.read
            .parquet(batterLegDir))
        } else {
          None
        }

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

        // TB, SLG, OBP, OPS, XBH, ISOP, GPA, GO/AO, BB/K
        val finalKboBatterYearlyDF = revisedKboBatterYearlyDF
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
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))


        /////////////////////////////////
        // legacy
        /////////////////////////////////
        val finalKboBatterLegDF = if (whichTime == "entire") {
          kboBatterLegDF.get
            .withColumn("data_type", lit("legacy"))
            .withColumn("player_id", col("id"))
            .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))
        } else {
          spark.emptyDataFrame
        }



        /////////////////////////////////
        // final
        /////////////////////////////////
        finalKboBatterYearlyDF.show
        finalKboBatterLegDF.show
        finalKboBatterDailyDF.show
//        finalKboBatterSitDF.show
        if ( whichTime == "current"){
          Seq(finalKboBatterYearlyDF,finalKboBatterDailyDF,finalKboBatterSitDF)

        }
        else {
          Seq(finalKboBatterYearlyDF,finalKboBatterDailyDF,finalKboBatterSitDF,finalKboBatterLegDF)
        }
      }
    }
  }

  def getKboPitcherData(kboDSDir : String): Seq[DataFrame] = {
    val pitcherDSDir : String = kboDSDir + GlobalConfig.PITCHER_DS_DIR_NAME
    val pitcherYearlyDir : String = pitcherDSDir + "/" + GlobalConfig.YEARLY_DS_DIR_NAME
    val pitcherDailyDir : String = pitcherDSDir + "/" + GlobalConfig.PITCHER_DAILY_DS_DIR_NAME
    val pitcherSitDir : String = pitcherDSDir + "/" + GlobalConfig.PITCHER_SITUATION_DS_DIR_NAME
    val pitcherLegDir : String = pitcherDSDir + "/" + GlobalConfig.LEGACY_DS_DIR_NAME


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

        val kboPitcherLegDF = spark.read
          .parquet(pitcherLegDir)
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
          )

        // W
        val dailyWinDF = kboPitcherDailyDF
          .withColumn("year", year(col("date")))
          .where(col("res") === "승")
          .groupBy("id", "year")
          .agg(
            count("date").alias("W")
          )

        // L
        val dailyLoseDF = kboPitcherDailyDF
          .withColumn("year", year(col("date")))
          .where(col("res") === "패")
          .groupBy("id", "year")
          .agg(
            count("date").alias("L")
          )

        // SV
        val dailySaveDF = kboPitcherDailyDF
          .withColumn("year", year(col("date")))
          .where(col("res") === "세")
          .groupBy("id", "year")
          .agg(
            count("date").alias("SV")
          )

        // HOLD
        val dailyHoldDF = kboPitcherDailyDF
          .withColumn("year", year(col("date")))
          .where(col("res") === "홀")
          .groupBy("id", "year")
          .agg(
            count("date").alias("HLD")
          )


        // yearly - join
        val revisedKboPitcherYearlyDF = kboPitcherYearlyDF
          .join(seasonERADF,Seq("id","year"), "left")
          .join(dailyGameDF,Seq("id","year"), "left")
          .join(dailyWinDF,Seq("id","year"), "left")
          .join(dailyLoseDF, Seq("id","year"), "left")
          .join(dailySaveDF, Seq("id","year"), "left")
          .join(dailyHoldDF, Seq("id","year"), "left")
          .na.fill(0)


        // WPCT, IP, WHIP, AVG, BABIP, P/G, P/IP, K/9, BB/9, K/BB, OBP, SLG, OPS
        val finalKboPitcherYearlyDF = revisedKboPitcherYearlyDF
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
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))

        /////////////////////////////////
        // legacy
        /////////////////////////////////
        val finalKboPitcherLegDF = kboPitcherLegDF
          .withColumn("data_type", lit("legacy"))
          .withColumn("player_id", col("id"))
          .withColumn("id", concat_ws("_", col("player_id"), col("year"), col("data_type")))


        /////////////////////////////////
        // final
        /////////////////////////////////
//        finalKboPitcherYearlyDF.show
//        finalKboPitcherDailyDF.show
//        finalKboPitcherSitDF.show
//        finalKboPitcherLegDF.show
        Seq(finalKboPitcherYearlyDF, finalKboPitcherDailyDF, finalKboPitcherSitDF, finalKboPitcherLegDF)
      }
    }
  }

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


  def main(args:Array[String]) : Unit = {
    try {

      val kboDSDir : String = "wasbs://"+secretEnv.AZURE_BLOB_OUTPUT_CONTAINER_NAME+"@"+secretEnv.AZURE_BLOB_STORAGE_ACCOUNT+".blob.core.windows.net/kbo-datasets/"
      val kboBatterData = getKboBatterData(kboDSDir, "entire")
      kboBatterData.foreach(x => storeDataToCosmos(x, "batter"))

      val kboPitcherData = getKboPitcherData(kboDSDir)
      kboPitcherData.foreach(x => storeDataToCosmos(x, "pitcher"))

      val kboFielderData = getKboFielderData(kboDSDir)
      kboFielderData.foreach(x => storeDataToCosmos(x, "fielder"))

      val kboRunnerData = getKboRunnerData(kboDSDir)
      kboRunnerData.foreach(x => storeDataToCosmos(x, "runner"))


//      getCurrentDF(kboDSDir)

    } catch {
      case ex : MyLittleException => println(ex.getMessage)
    } finally {
      sparkManager.stopSpark
    }
  }
}