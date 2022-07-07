package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

object CombinerH extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerH")
    .master("local[*]")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()

  def transformationFunction(in: TaskInput): TaskOutput = {
    val review = in.getTableFrame("staging", "yelp_db", "table_a_parq").as("review")
    val user = in.getTableFrame("staging", "yelp_db", "table_g").as("user")
      .drop("funny", "cool", "useful")
      .withColumnRenamed("id", "user_id")

    TaskOutput(review.join(user, "user_id"))
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("yelp_test/combining/yelp_db/table_h/table_h_1.0.yml", transformationFunction)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 2, 0, 0),
    LocalDateTime.of(2012, 5, 3, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
    LocalDateTime.of(2012, 5, 5, 0, 0)
  )
  val results = wrapper.get.runTasksAndWait(runTimes)
  logger.info(results)
}
