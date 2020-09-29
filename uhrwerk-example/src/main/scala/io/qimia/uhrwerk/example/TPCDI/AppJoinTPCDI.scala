package io.qimia.uhrwerk.example.TPCDI

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.UhrwerkAppRunner
import org.apache.spark.sql.SparkSession

object AppJoinTPCDI extends App {


  val sparkSess = SparkSession
    .builder()
    .appName("LoaderTPCDI")
    .master("local[*]")
    .config("driver-memory", "2g")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "./docker/spark_logs")
    .getOrCreate()


  UhrwerkAppRunner.runDagFile(sparkSess,
    "TPCDI/env-config.yml",
    "TPCDI/dag_tpcdi.yml",
    Array(
      TableIdent("processing", "tpcdi", "Trades", "1.0")
    ),
    Option(LocalDateTime.of(2015, 7, 8, 0, 0)),
    Option(LocalDateTime.of(2015, 7, 10, 0, 0)),
    dagMode = true,
    10,
    overwrite = true
  )


}
