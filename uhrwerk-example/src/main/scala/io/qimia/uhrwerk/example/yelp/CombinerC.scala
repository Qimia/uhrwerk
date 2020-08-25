package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import java.util.concurrent.Executors

import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext

object CombinerC extends App {

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerC")
    .master("local[3]")
    .getOrCreate()

  def CombinerCFunc(in: TaskInput): DataFrame = {
    // The most basic userFunction simply returns the input dataframe
    val aDF = in.inputFrames.values.head
    val bDF = in.inputFrames.values.tail.head
    aDF
      .withColumn("new_id", monotonically_increasing_id())
      .join(bDF.withColumn("new_id", monotonically_increasing_id()),
            "new_id" :: Nil)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTable("combiner-C-parq.yml", CombinerCFunc, true)

  val runTimes = Array(LocalDateTime.of(2012, 5, 1, 0, 0))
  val singleExecutor = Executors.newSingleThreadExecutor()
  implicit val executorRunner = ExecutionContext.fromExecutor(singleExecutor)
  val results = wrapper.get.runTasksAndWait(runTimes, false)
  println(results)

  sparkSess.close()

}
