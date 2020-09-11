package io.qimia.uhrwerk.example.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.SourceIdent
import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}

object Simple_Loading_MultipleTargets extends App {
  val sparkSess = SparkSession.builder()
    .appName("loading_multiple_targets")
    .master("local")
    .getOrCreate()

  def udf(in: TaskInput): DataFrame =  {
    val ident = new SourceIdent("retail_mysql", "qimia_oltp.sales", "jdbc")
    in.inputFrames.get(ident) match {
      case Some(x) => x
      case None => throw new Exception("Table not found!")
    }
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("retail_examples/simple_loading_multiple_targets.yml", udf, true)

  val runTimes = Array(
    LocalDateTime.of(2020, 6, 1, 0, 0),
    LocalDateTime.of(2020, 6, 2, 0, 0),
    LocalDateTime.of(2020, 6, 3, 0, 0)
  )

  val results = wrapper.get.runTasksAndWait(runTimes)
  results
}
