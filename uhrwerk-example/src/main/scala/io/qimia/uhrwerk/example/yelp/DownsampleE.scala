package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}

object DownsampleE  extends App {
  val sparkSess = SparkSession.builder()
    .appName("DownsampleE")
    .master("local")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)
//  Logger.getLogger("akka").setLevel(Level.ERROR)


  def loaderAFunc(in: TaskInput): TaskOutput = {
    val aDF = in.loadedInputFrames.values.head
    aDF.select("date").agg(min("date"), max("date")).show()
    aDF.printSchema()
    aDF.show(10)
    TaskOutput(aDF)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("downsample-E.yml", loaderAFunc, true)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
  )
  if (wrapper.isDefined) {
    val results = wrapper.get.runTasksAndWait(runTimes, false)
    println(results)
  }
}
