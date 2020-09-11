package io.qimia.uhrwerk.example.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.{SourceIdent, TableIdent}
import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}

object Loading_Joining extends App {

  val sparkSess = SparkSession.builder()
    .appName("load_join_dependency")
    .master("local")
    .getOrCreate()

  val frameManager = new SparkFrameManager(sparkSess)

  def udf(in: TaskInput): DataFrame = {
    val salesItemsIdent = new SourceIdent("retail_mysql", "qimia_oltp.sales_items", "jdbc")

    in.inputFrames.get(salesItemsIdent) match {
      case Some(x) => x
      case None => throw new Exception("Table not found!")
    }
  }

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("retail_examples/loading_joining.yml", udf)

  val runTimes = Array(
    LocalDateTime.of(2020, 6, 1, 0, 0),
    LocalDateTime.of(2020, 6, 2, 0, 0),
    LocalDateTime.of(2020, 6, 3, 0, 0)
  )

  val result = wrapper.get.runTasksAndWait(runTimes)
  println(result)
}
