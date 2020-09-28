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
    "yelp_test/uhrwerk.yml",
    Array("yelp_test/testing-connection-config.yml"),
    Array("yelp_test/staging/yelp_db/table_a_parq_app/table_a_parq_app_1.0.yml"),
    TableIdent("staging", "yelp_db", "table_a_parq_app", "1.0"),
    Option(LocalDateTime.of(2012, 5, 1, 0, 0)),
    Option(LocalDateTime.of(2012, 5, 6, 0, 0)),
    dagMode = false,
    1,
    overwrite = false
  )

}
