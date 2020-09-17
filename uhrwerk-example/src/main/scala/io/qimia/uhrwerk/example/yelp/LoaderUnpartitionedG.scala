package io.qimia.uhrwerk.example.yelp

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LoaderUnpartitionedG extends App {
  val sparkSess = SparkSession.builder()
    .appName("loaderUnpartitionedG")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def loaderUnpartitionedGFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    in.loadedInputFrames.values.head.show()
    TaskOutput(in.loadedInputFrames.values.head)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("loader-unpartitioned-G.yml", loaderUnpartitionedGFunc)

  val results = wrapper.get.runTasksAndWait()
  println(results)
}
