package akbo.main

import akbo.GlobalConfig
import akbo.handler.cosmosHandler
import akbo.handler.teamHandler
import exception.MyLittleException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import secret.secretEnv
import sparkManager.sparkManager
import akbo.handler.batterHandler
import akbo.handler.pitcherHandler
import akbo.handler.runnerHandler
import akbo.handler.fielderHandler

object statMain{

  def main(args:Array[String]) : Unit = {
    try {
      val kboDSDir : String = secretEnv.CURRENT_KBO_DS_DIR

      val kboFielderData = fielderHandler.getKboFielderData(kboDSDir)
      kboFielderData.foreach(x => cosmosHandler.storeDataToCosmos(x, "fielder"))

      val kboRunnerData = runnerHandler.getKboRunnerData(kboDSDir)
      kboRunnerData.foreach(x => cosmosHandler.storeDataToCosmos(x, "runner"))

      val kboTeamData = teamHandler.getTeamData(kboDSDir)
      kboTeamData.foreach(x => cosmosHandler.storeDataToCosmos(x, "team"))

      val kboBatterData = batterHandler.getKboBatterData(kboDSDir, "entire",kboFielderData(0).select(col("year"),col("player_id"),col("POS").alias("position")))
      kboBatterData.tail.foreach(x => cosmosHandler.storeDataToCosmos(x, "batter"))

      val kboPitcherData = pitcherHandler.getKboPitcherData(kboDSDir, kboFielderData(0).select(col("year"),col("player_id"),col("POS").alias("position")))
      kboPitcherData.tail.foreach(x => cosmosHandler.storeDataToCosmos(x, "pitcher"))

      val kboConsolidatedPlayers = Seq(kboBatterData.head, kboPitcherData.head)
      kboConsolidatedPlayers.foreach(x => cosmosHandler.storeDataToCosmos(x, "consolidated"))

    } catch {
      case ex : MyLittleException => println(ex.getMessage)
    } finally {
      sparkManager.stopSpark
    }
  }
}