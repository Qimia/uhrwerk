package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object CombinerH extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerH")
    .master("local[*]")
    .getOrCreate()

  def transformationFunction(in: TaskInput): TaskOutput = {
    val review = in.getTableFrame("staging", "yelp_db", "table_a_parq").as("review")
    val user = in.getTableFrame("staging", "yelp_db", "table_g").as("user")
      .drop("funny", "cool", "useful")
      .withColumnRenamed("id", "user_id")

    TaskOutput(review.join(user, "user_id"))
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("combiner-H.yml", transformationFunction)

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
