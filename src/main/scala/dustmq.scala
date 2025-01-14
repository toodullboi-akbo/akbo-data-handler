import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.BasicFileAttributes
import scala.jdk.CollectionConverters._
import java.net.URLClassLoader

import sparkManager.sparkManager
import exception.MyLittleException
import fileIO.fileManager
import secret.secretEnv

object dustmq{
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


  def getKboBatterData(kboDSDir : String): Unit = {
    val batterDSDir: String = kboDSDir + GlobalConfig.BATTER_DS_DIR_NAME
    val batterDailyDir : String = kboDSDir + GlobalConfig.BATTER_DS_DIR_NAME + "/" + GlobalConfig.BATTER_DAILY_DS_DIR_NAME
    val batterSitDir : String = kboDSDir + GlobalConfig.BATTER_DS_DIR_NAME +"/" + GlobalConfig.BATTER_SITUATION_DS_DIR_NAME

//    val batterDSDir : String = Paths.get(kboDSDir, GlobalConfig.BATTER_DS_DIR_NAME).toString
//    val batterDailyDir : String = Paths.get(batterDSDir, GlobalConfig.BATTER_DAILY_DS_DIR_NAME).toString
//    val batterSitDir : String = Paths.get(batterDSDir, GlobalConfig.BATTER_SITUATION_DS_DIR_NAME).toString

    sparkManager.withSparkSession{
      (spark) =>{
        val kboBatterYearlyDF = spark.read
          .parquet(batterDSDir)

        val kboBatterDailyDF = spark.read
          .option("dateFormat","yyyy-MM-dd")
          .parquet(batterDailyDir)

        val kboBatterSitDF = spark.read
          .parquet(batterSitDir)

        /////////////////////////////////
        // yearly
        /////////////////////////////////
        // AVG // of daily game
        val windowSpec = Window.partitionBy("id").orderBy(desc("date"))
        val seasonAVGDF = kboBatterDailyDF
          .withColumn("rowNo",row_number.over(windowSpec))
          .where(col("rowNo") === 1)
          .select(col("id"),col("seasonAVG").alias("AVG"))

        // G,PA,AB,R,H,2B,3B,HR,RBI,BB,HBP,SO,GDP // sum ( or counting ) of daily game
        val dailyGameDF = kboBatterDailyDF
          .groupBy("id")
          .agg(
            count("date").alias("G"),
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
          .join(dailyGameDF,"id", "left")
          .join(seasonAVGDF,"id", "left")

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

        /////////////////////////////////
        // daily
        /////////////////////////////////
        // gameAVG - merge
        val finalKboBatterDailyDF = kboBatterDailyDF
          .withColumn("gameAVG", round(col("H")/col("AB"),3))

        /////////////////////////////////
        // situation
        /////////////////////////////////
        val finalKboBatterSitDF = kboBatterSitDF

        /////////////////////////////////
        // final
        /////////////////////////////////
        finalKboBatterYearlyDF.show
        finalKboBatterDailyDF.show
        finalKboBatterSitDF.show
      }
    }
  }

  def getKboPitcherData(kboDSDir : String): Unit = {
    val pitcherDSDir : String = Paths.get(kboDSDir, GlobalConfig.PITCHER_DS_DIR_NAME).toString
    val pitcherDailyDir : String = Paths.get(pitcherDSDir, GlobalConfig.PITCHER_DAILY_DS_DIR_NAME).toString
    val pitcherSitDir : String = Paths.get(pitcherDSDir, GlobalConfig.PITCHER_SITUATION_DS_DIR_NAME).toString

    sparkManager.withSparkSession {
      (spark) => {
        val kboPitcherYearlyDF = spark.read
          .parquet(pitcherDSDir)

        val kboPitcherDailyDF = spark.read
          .option("dateFormat","yyyy-MM-dd")
          .parquet(pitcherDailyDir)
          .withColumn("tempFloatIP", round(col("IP"),3))
          .drop("IP")
          .withColumnRenamed("tempFloatIP","IP")

        val kboPitcherSitDF = spark.read
          .parquet(pitcherSitDir)

        /////////////////////////////////
        // yearly
        /////////////////////////////////
        // ERA
        val windowSpec = Window.partitionBy("id").orderBy(desc("date"))
        val seasonERADF = kboPitcherDailyDF
          .withColumn("rowNo",row_number.over(windowSpec))
          .where(col("rowNo") === 1)
          .select(col("id"),col("seasonERA").alias("ERA"))
        // G, tempIP, H, HR, BB, HBP, SO, R, ER
        val dailyGameDF = kboPitcherDailyDF
          .groupBy("id")
          .agg(
            count("date").alias("G"),
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
          .where(col("res") === "승")
          .groupBy("id")
          .agg(
            count("date").alias("W")
          )

        // L
        val dailyLoseDF = kboPitcherDailyDF
          .where(col("res") === "패")
          .groupBy("id")
          .agg(
            count("date").alias("L")
          )

        // SV
        val dailySaveDF = kboPitcherDailyDF
          .where(col("res") === "세")
          .groupBy("id")
          .agg(
            count("date").alias("SV")
          )

        // HOLD
        val dailyHoldDF = kboPitcherDailyDF
          .where(col("res") === "홀")
          .groupBy("id")
          .agg(
            count("date").alias("HLD")
          )


        // yearly - join
        val revisedKboPitcherYearlyDF = kboPitcherYearlyDF
          .join(seasonERADF,"id", "left")
          .join(dailyGameDF,"id", "left")
          .join(dailyWinDF, "id", "left")
          .join(dailyLoseDF, "id", "left")
          .join(dailySaveDF, "id", "left")
          .join(dailyHoldDF, "id", "left")
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

        /////////////////////////////////
        // daily
        /////////////////////////////////
        val finalKboPitcherDailyDF = kboPitcherDailyDF

        /////////////////////////////////
        // situation
        /////////////////////////////////
        val finalKboPitcherSitDF = kboPitcherSitDF


        /////////////////////////////////
        // final
        /////////////////////////////////
        finalKboPitcherYearlyDF.show
        finalKboPitcherDailyDF.show
        finalKboPitcherSitDF.show
      }
    }
  }

  def getKboFielderData(kboDSDir : String): Unit = {
    val fielderDSDir : String = Paths.get(kboDSDir, GlobalConfig.FIELDER_DS_DIR_NAME).toString

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

        finalKboFielderYearlyDF.show
      }
    }
  }

  def getKboRunnerData(kboDSDir : String): Unit = {
    val runnerDSDir : String = Paths.get(kboDSDir, GlobalConfig.RUNNER_DS_DIR_NAME).toString

    sparkManager.withSparkSession {
      (spark) => {
        val kboRunnerYearlyDF = spark.read
          .parquet(runnerDSDir)

        kboRunnerYearlyDF
          .withColumn("SBA", col("SB")+col("CS"))
          .withColumn("SB_per", round(col("SB")/col("SBA")*100,1))
          .show
      }
    }

  }


  def main(args:Array[String]) : Unit = {
    try {
      val classLoader = this.getClass.getClassLoader

      classLoader match {
        case urlClassLoader: URLClassLoader =>
          val urls = urlClassLoader.getURLs
          println("Dependencies loaded at runtime:")
          urls.foreach(url => println(url.getPath))
        case _ =>
          println("ClassLoader is not a URLClassLoader, dependencies cannot be listed.")
      }

      val kboDSDir : String = "wasbs://"+secretEnv.AZURE_BLOB_OUTPUT_CONTAINER_NAME+"@"+secretEnv.AZURE_BLOB_STORAGE_ACCOUNT+".blob.core.windows.net/kbo-datasets/"
//        val kboDSDir : String = s"abfss://${secretEnv.AZURE_BLOB_OUTPUT_CONTAINER_NAME}@${secretEnv.AZURE_BLOB_STORAGE_ACCOUNT}.dfs.core.windows.net/kbo-datasets/"
//      azureBlobStorageConnector.printListOfBlob()
//      val kboDSDir : String = args(0)
        getKboBatterData(kboDSDir)
//      getKboPitcherData(kboDSDir)
//      getKboFielderData(kboDSDir)
//      getKboRunnerData(kboDSDir)
    } catch {
      case ex : MyLittleException => println(ex.getMessage)
    } finally {
      sparkManager.stopSpark
    }
  }
}