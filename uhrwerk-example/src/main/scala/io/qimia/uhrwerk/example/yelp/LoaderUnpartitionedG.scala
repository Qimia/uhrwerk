package io.qimia.uhrwerk.example.yelp

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoaderUnpartitionedG extends App {
  val sparkSess = SparkSession.builder()
    .appName("loaderUnpartitionedG")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def loaderUnpartitionedGFunc(in: TaskInput): DataFrame = {
    // The most basic userFunction simply returns the input dataframe
    in.inputFrames.values.head.show()
    in.inputFrames.values.head
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("loader-unpartitioned-G.yml", loaderUnpartitionedGFunc)

  val results = wrapper.get.runTasksAndWait()
  println(results)
}
