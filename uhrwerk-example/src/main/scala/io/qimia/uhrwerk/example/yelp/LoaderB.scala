package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Files


object LoaderB extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession.builder()
    .appName("loaderA")
    .master("local")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()

  def loaderBFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    TaskOutput(in.loadedInputFrames.values.head)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_b/table_b_1.0.yml", loaderBFunc)

  val runTimes = Array(LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 2, 3, 4))
  val results = wrapper.get.runTasksAndWait(runTimes)
  logger.info(results)

}
