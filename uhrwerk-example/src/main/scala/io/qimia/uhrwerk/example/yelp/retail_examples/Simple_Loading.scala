package io.qimia.uhrwerk.example.yelp.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}

object Simple_Loading extends App {
  val sparkSess = SparkSession.builder()
    .appName("Retail_SimpleLoading")
    .master("local")
    .getOrCreate()

  def transformationFunction(in: TaskInput): DataFrame = {
    in.inputFrames.values.head
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("retail_examples/simple_loading.yml", transformationFunction)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 8, 0, 0),
    LocalDateTime.of(2012, 5, 15, 0, 0),
    LocalDateTime.of(2012, 5, 22, 0, 0)
  )

  val results = wrapper.get.runTasksAndWait(runTimes)
  println(results)
}
