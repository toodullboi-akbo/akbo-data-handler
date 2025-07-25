package akbo.handler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import secret.secretEnv
import sparkManager.sparkManager

object cosmosHandler {
  def storeDataToCosmos(df : DataFrame, containerName : String): Unit = {
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
             |  'manualThroughput' = '400'
             |)
             |""".stripMargin
        )


        // check existence
        var existingDF : DataFrame = spark.emptyDataFrame;


        if(dataType == "daily") {
          existingDF = spark.read
            .format("cosmos.oltp")
            .option("dateFormat", "yyyy-MM-dd")
            .options(secretEnv.AZURE_COSMOS_CONFIG
              + ("spark.cosmos.container" -> containerName)
              + ("spark.cosmos.read.customQuery" -> s"SELECT * FROM c WHERE c.data_type ='${dataType}'")
            )
            .load()
            .withColumn("date", expr("date_add('1970-01-01', date)"))  // Convert INT to DATE


        } else {
          existingDF = spark.read
            .format("cosmos.oltp")
            .options(secretEnv.AZURE_COSMOS_CONFIG
              + ("spark.cosmos.container" -> containerName)
              + ("spark.cosmos.read.customQuery" -> s"SELECT * FROM c WHERE c.data_type ='${dataType}'")
            )
            .load()
        }


        if (existingDF.isEmpty){
          // first inserting
          df.write
            .format("cosmos.oltp")
            .options(
              secretEnv.AZURE_COSMOS_CONFIG
                + ("spark.cosmos.container" -> containerName)
                + ("spark.cosmos.write.strategy" -> "ItemOverwrite")
            )
            .mode("APPEND")
            .save()

        } else {
          // field-level compare
          val fieldToCompare = df.columns
          val changedCondition = fieldToCompare.map(field => col(s"new.${field}") =!= col(s"old.${field}"))
          val joinedDF = df.alias("new")
            .join(existingDF.alias("old"), Seq("id"), "inner")
          val changedDF = joinedDF.filter(changedCondition.reduce(_ || _))
            .select("new.*")

          // records-level compare
          val newRecordsDF = df.alias("new")
            .join(existingDF.alias("old"),
              Seq("id"),
              "leftanti"
            )


          val finalUpdateDF = newRecordsDF.union(changedDF)

          if (!finalUpdateDF.isEmpty) {
            finalUpdateDF.write
              .format("cosmos.oltp")
              .options(
                secretEnv.AZURE_COSMOS_CONFIG
                  + ("spark.cosmos.container" -> containerName)
                  + ("spark.cosmos.write.strategy" -> "ItemOverwrite")
              )
              .mode("APPEND")
              .save()
          }
        }
      }
    }
  }
}
