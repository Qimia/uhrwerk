package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}

import java.nio.file.Files

object DownsampleE  extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession.builder()
    .appName("DownsampleE")
    .master("local")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()

  def loaderEFunc(in: TaskInput): TaskOutput = {
    val aDF = in.loadedInputFrames.values.head
    aDF.select("date").agg(min("date"), max("date")).show()
    aDF.printSchema()
    aDF.show(10)
    TaskOutput(aDF)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("yelp_test/combining/yelp_db/table_e/table_e_1.0.yml", loaderEFunc, overwrite = true)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
  )
  if (wrapper.isDefined) {
    val results = wrapper.get.runTasksAndWait(runTimes, overwrite = false)
    logger.info(results)
  }
}
