package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object LoaderAParq extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val sparkSess = SparkSession.builder()
    .appName("LoaderAParq")
    .getOrCreate()

  def loaderAFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    val aDF = in.loadedInputFrames.values.head
    aDF.select("day").agg(min("day"), max("day")).show()
    aDF.printSchema()
    aDF.show(10)
    TaskOutput(aDF)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config-docker.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config-docker.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("loader-A-parq.yml", loaderAFunc, true)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 2, 0, 0),
    LocalDateTime.of(2012, 5, 3, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
    LocalDateTime.of(2012, 5, 5, 0, 0)
  )
  val results = wrapper.get.runTasksAndWait(runTimes, false)
  logger.info(results)
}
