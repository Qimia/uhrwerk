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
      TableIdent("staging", "tpcdi", "accounts", "1.0"),
      TableIdent("staging", "tpcdi", "DIM_StatusType", "1.0")
    ),
    Option(LocalDateTime.of(2007, 7, 3, 0, 0)),
    Option(LocalDateTime.of(2007, 7, 10, 0, 0)),
    false,
    1,
    true
  )

}
