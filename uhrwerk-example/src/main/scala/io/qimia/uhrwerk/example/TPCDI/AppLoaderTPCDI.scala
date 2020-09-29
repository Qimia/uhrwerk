package io.qimia.uhrwerk.example.TPCDI

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.UhrwerkAppRunner
import org.apache.spark.sql.SparkSession

object AppLoaderTPCDI extends App {

  val sparkSess = SparkSession.builder()
    .appName("loaderTPCDI")
    .master("local")
    .getOrCreate()

  UhrwerkAppRunner.runDagFile(sparkSess,
    "TPCDI/env-config.yml",
    "TPCDI/dag_tpcdi.yml",
    Array(
      TableIdent("staging", "tpcdi", "Accounts", "1.0"),
      TableIdent("staging", "tpcdi", "CustomerMgmt", "1.0"),
      TableIdent("staging", "tpcdi", "DailyMarket", "1.0"),
      TableIdent("staging", "tpcdi", "Fact_Trade", "1.0"),
      TableIdent("staging", "tpcdi", "DIM_StatusType", "1.0"),
      TableIdent("staging", "tpcdi", "DIM_TradeType", "1.0"),
      TableIdent("staging", "tpcdi", "Dim_Date", "1.0")
    ),
    Option(LocalDateTime.of(2015, 7, 6, 0, 0)),
    Option(LocalDateTime.of(2015, 8, 6, 0, 0)),
    dagMode = true,
    10,
    overwrite = false
  )

}
