package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import java.util.concurrent.Executors

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext

object LoaderAParq extends App {

  val sparkSess = SparkSession.builder()
    .appName("loaderA")
    .master("local[3]")
    .getOrCreate()

  def loaderAFunc(in: TaskInput): DataFrame = {
    // The most basic userFunction simply returns the input dataframe
    val aDF = in.inputFrames.values.head
    aDF.printSchema()
    aDF.show(10)
    aDF
  }

  val frameManager = new SparkFrameManager(sparkSess)

  // TODO: Needs framemanager
  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("loader-A-parq.yml", loaderAFunc)

  // Uncomment and test as soon as framemanager has been merged (test BulkDependencyResult conversions before that)
  val runTimes = Array(LocalDateTime.of(2012, 5, 1, 0, 0))
  val singleExecutor = Executors.newSingleThreadExecutor()
  implicit val executorRunner = ExecutionContext.fromExecutor(singleExecutor)
  wrapper.get.runTasks(runTimes)

}
