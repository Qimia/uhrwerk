package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.nio.file.Files


object LoaderAParq extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession.builder()
    .appName("LoaderAParq")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
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
  val wrapper = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_a_parq/table_a_parq_1.0.yml", loaderAFunc, overwrite = true)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 2, 0, 0),
    LocalDateTime.of(2012, 5, 3, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
    LocalDateTime.of(2012, 5, 5, 0, 0)
  )
  val results = wrapper.get.runTasksAndWait(runTimes, overwrite = false)
  logger.info(results)
}
