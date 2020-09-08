package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}



object LoaderA extends App {

  val sparkSess = SparkSession.builder()
    .appName("loaderA")
    .master("local")
    .getOrCreate()

  def loaderAFunc(in: TaskInput): DataFrame = {
    // The most basic userFunction simply returns the input dataframe
    in.inputFrames.values.head
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml" ,frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("loader-A.yml", loaderAFunc)

  val runTimes = Array(LocalDateTime.of(2012, 5, 1, 0, 0))
  val results = wrapper.get.runTasksAndWait(runTimes)
  println(results)
}
