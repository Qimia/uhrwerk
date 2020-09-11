package io.qimia.uhrwerk.example.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.SourceIdent
import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}

object Simple_Loading_toJDBC extends App {
  val sparkSess = SparkSession.builder()
    .appName("Retail_SimpleLoading")
    .master("local")
    .getOrCreate()

  val frameManager = new SparkFrameManager(sparkSess)

  def udf(in: TaskInput): DataFrame = {
    //in.inputFrames.values.head
    val ident = new SourceIdent("retail_mysql","qimia_oltp.sales","jdbc")
    in.inputFrames.get(ident) match {
      case Some(x) => x
      case None => throw new Exception("Table not found!")
    }
  }

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("retail_examples/simple_loading_tojdbc.yml", udf, true)

  val runTimes = Array(
    LocalDateTime.of(2020, 6, 1, 0, 0),
    LocalDateTime.of(2020, 6, 2, 0, 0),
    LocalDateTime.of(2020, 6, 3, 0, 0)
  )

  val results = wrapper.get.runTasksAndWait(runTimes, true)
  println(results)
}
