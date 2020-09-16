package io.qimia.uhrwerk.example.yelp

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CombinerI extends App {
  val sparkSess = SparkSession
    .builder()
    .appName("CombinerI")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def transformationFunction(in: TaskInput): DataFrame = {
    in.inputFrames.head._2
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("combiner-I.yml", transformationFunction)

  val results = wrapper.get.runTasksAndWait()
  println(results)
}
