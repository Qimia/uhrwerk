package io.qimia.uhrwerk.example.TPCDI

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.UhrwerkAppRunner
import org.apache.spark.sql.SparkSession

object AppLoaderAccounts extends App {

  val sparkSess = SparkSession.builder()
    .appName("loaderTPCDI")
    .master("local")
    .getOrCreate()

  UhrwerkAppRunner.runFiles(
    sparkSess,
    "TPCDI/env-config.yml",
    Array("TPCDI/connection-config.yml"),
    Array("TPCDI/LoadAccounts.yml"),
    TableIdent("staging", "tpcdi", "accounts", "1.0"),
    LocalDateTime.of(2007, 7, 3, 0, 0),
    LocalDateTime.of(2007, 7, 10, 0, 0),
    false,
    1,
    true

  )

}
