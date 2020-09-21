package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.UhrwerkAppRunner
import org.apache.spark.sql.SparkSession

object AppLoaderAParq extends App {
  val sparkSess = SparkSession.builder()
    .appName("loaderA")
    .master("local[3]")
    .getOrCreate()

  UhrwerkAppRunner.runFiles(
    sparkSess,
    "testing-env-config.yml",
    Array("testing-connection-config.yml"),
    Array("loader-A-parq-app.yml"),
    TableIdent("staging", "yelp_db", "table_a_parq", "1.0"),
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 6, 0, 0),
    false,
    1,
    false
  )

}