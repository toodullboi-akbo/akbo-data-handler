import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.BasicFileAttributes
import scala.jdk.CollectionConverters._

import sparkManager.sparkManager
import exception.MyLittleException
import fileIO.fileManager

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


  def doKboBatterData(kboDSDir : String): Unit = {
    val batterDSDir : String = Paths.get(kboDSDir, GlobalConfig.BATTER_DS_DIR_NAME).toString
    val batterDailyDir : String = Paths.get(batterDSDir, GlobalConfig.BATTER_DAILY_DS_DIR_NAME).toString
    val batterSitDir : String = Paths.get(batterDSDir, GlobalConfig.BATTER_SITUATION_DS_DIR_NAME).toString

    sparkManager.withSparkSession{
      (spark) =>{
        val kboBatterYearlyDF = spark.read
          .parquet(batterDSDir)

        kboBatterYearlyDF.show(truncate = true)
        val kboBatterDailyDF = spark.read
          .option("dateFormat","yyyy-MM-dd")
          .parquet(batterDailyDir)

        val kboBatterSitDF = spark.read
          .parquet(batterSitDir)

        /////////////////////////////////
        // daily
        /////////////////////////////////
        // gameAVG - merge
        val finalKboBatterDailyDF = kboBatterDailyDF
          .withColumn("gameAVG", round(col("H")/col("AB"),3))

        /////////////////////////////////
        // yearly
        /////////////////////////////////
        // AVG
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

  def main(args:Array[String]) : Unit = {
    try {
      val kboDSDir : String = args(0)
      doKboBatterData(kboDSDir)

    } catch {
      case ex : MyLittleException => println(ex.getMessage)
    } finally {
      sparkManager.stopSpark()
    }
  }
}