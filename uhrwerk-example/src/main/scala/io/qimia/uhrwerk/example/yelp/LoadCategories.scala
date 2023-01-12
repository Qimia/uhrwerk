package io.qimia.uhrwerk.example.yelp

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Files
import java.time.LocalDateTime

object LoadCategories extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val tmpDir = Files.createTempDirectory("spark-events")
  logger.info(s"Created directory: ${tmpDir.toAbsolutePath.toString}")

  val sparkSess = SparkSession
    .builder()
    .appName("LoaderA")
    .master("local[*]")
    .config("driver-memory", "4g")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()

  def loaderAFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    TaskOutput(in.loadedInputFrames.values.head)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("yelp_test/env-secrets-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile(
    "yelp_test/connection-secrets-config.yml",
    false
  )
  val wrapper = uhrwerkEnvironment.addTableFile(
    "yelp_test/staging/yelp_db/table_category/table_category_1.0.yml",
    userFunc = loaderAFunc,
    overwrite = false
  )

  val runTimes =
    List(LocalDateTime.of(2012, 5, 1, 0, 0), LocalDateTime.of(2012, 5, 2, 3, 4))
  val results = wrapper.get.runTasksAndWait(List[LocalDateTime]())
  logger.info(results)
}