package io.qimia.uhrwerk.example.yelp

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CombinerI extends App {
  val sparkSess = SparkSession
    .builder()
    .appName("CombinerI")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def transformationFunction(in: TaskInput): TaskOutput = {
    TaskOutput(in.getFrameByName("table_g"))
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("combiner-I.yml", transformationFunction)

  val results = wrapper.get.runTasksAndWait()
  println(results)
}
