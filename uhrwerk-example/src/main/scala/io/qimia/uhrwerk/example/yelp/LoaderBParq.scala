package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}

import java.nio.file.Files


object LoaderBParq extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession.builder()
    .appName("loaderB")
    .master("local[3]")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()

  def loaderBFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    val aDF = in.loadedInputFrames.values.head
    aDF.select("day").agg(min("day"), max("day")).show()
    aDF.printSchema()
    aDF.show(10)
    TaskOutput(aDF)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_b_parq/table_b_parq_1.0.yml", loaderBFunc, overwrite = true)

  val runTimes = List(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 2, 0, 0),
    LocalDateTime.of(2012, 5, 3, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
    LocalDateTime.of(2012, 5, 5, 0, 0)
  )
  val results = wrapper.get.runTasksAndWait(runTimes, overwrite = false)
  logger.info(results)

}
