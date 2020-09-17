package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CombinerH extends App {
  val sparkSess = SparkSession
    .builder()
    .appName("CombinerH")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def transformationFunction(in: TaskInput): TaskOutput = {
    val review = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("table_a_parq")).get._2.as("review")
    val user = in.loadedInputFrames
      .find(t => t._1.asInstanceOf[TableIdent].name.equals("table_g"))
      .get
      ._2
      .as("user")
      .drop("funny", "cool", "useful")
      .withColumnRenamed("id", "user_id")

    TaskOutput(review.join(user, "user_id"))
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("combiner-H.yml", transformationFunction)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 2, 0, 0),
    LocalDateTime.of(2012, 5, 3, 0, 0),
    LocalDateTime.of(2012, 5, 4, 0, 0),
    LocalDateTime.of(2012, 5, 5, 0, 0)
  )
  val results = wrapper.get.runTasksAndWait(runTimes)
  println(results)
}
