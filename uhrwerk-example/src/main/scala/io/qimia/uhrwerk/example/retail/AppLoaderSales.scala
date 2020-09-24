package io.qimia.uhrwerk.example.retail

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput, UhrwerkAppRunner}
import io.qimia.uhrwerk.example.yelp.AppLoaderAParq.sparkSess
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

object AppLoaderSales extends App {

  val sparkSess = SparkSession.builder()
    .appName("loaderA")
    .master("local")
    .getOrCreate()

  UhrwerkAppRunner.runFiles(
    sparkSess,
    "testing-env-config.yml",
    Array("testing-connection-config.yml"),
    Array("LoadTableSalesTest.yml"),
    TableIdent("staging", "qimia_oltp", "sales", "1.0"),
    Option(LocalDateTime.of(2020, 6, 1, 0, 0)),
    Option(LocalDateTime.of(2020, 6, 5, 0, 0)),
    false,
    1,
    false
  )

}
