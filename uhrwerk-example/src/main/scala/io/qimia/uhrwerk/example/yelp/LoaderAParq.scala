package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions._


object LoaderAParq extends App {

  val sparkSess = SparkSession.builder()
    .appName("loaderA")
    .master("local[3]")
    .getOrCreate()

  def loaderAFunc(in: TaskInput): DataFrame = {
    // The most basic userFunction simply returns the input dataframe
    val aDF = in.inputFrames.values.head
    aDF.select("day").agg(min("day"), max("day")).show()
    aDF.printSchema()
    aDF.show(10)
    aDF
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("loader-A-parq.yml", loaderAFunc, true)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 2, 0, 0),
    LocalDateTime.of(2012, 5, 3, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
    LocalDateTime.of(2012, 5, 5, 0, 0)
  )
  val results = wrapper.get.runTasksAndWait(runTimes, false)
  println(results)
}
