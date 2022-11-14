package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import io.qimia.uhrwerk.framemanager.utils.SparkFrameManagerUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}

import java.nio.file.Files

object WindowF extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession.builder()
    .appName("WindowF")
    .master("local")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()

  def loaderAFunc(in: TaskInput): TaskOutput = {
    val aDF = in.loadedInputFrames.values.head
    aDF.select("date").agg(min("date"), max("date")).show()
    aDF.printSchema()
    aDF.show(10)
    TaskOutput(aDF.drop(SparkFrameManagerUtils.timeColumns: _*))
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("yelp_test/combining/yelp_db/table_f/table_f_1.0.yml", loaderAFunc, overwrite = true)

  val runTimes = List(
    LocalDateTime.of(2012, 5, 3, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
    LocalDateTime.of(2012, 5, 5, 0, 0),
    LocalDateTime.of(2012, 5, 6, 0, 0)
  )
  if (wrapper.isDefined) {
    val results = wrapper.get.runTasksAndWait(runTimes, overwrite = false)
    logger.info(results)
  }
}
