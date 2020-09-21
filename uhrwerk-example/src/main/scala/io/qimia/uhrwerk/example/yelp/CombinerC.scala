package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object CombinerC extends App {

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerC")
    .master("local[3]")
    .getOrCreate()

  def CombinerCFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    val aDF = in.loadedInputFrames(TableIdent("staging", "yelp_db", "table_a", "1.0"))
      .drop("id", "user_id", "year", "month", "day", "hour", "minute", "date")
    val bDF = in.loadedInputFrames(TableIdent("staging", "yelp_db", "table_b", "1.0"))
      .drop("id", "business_id")
      .withColumnRenamed("text", "othertext")
    val outDF = aDF
      .withColumn("new_id", monotonically_increasing_id())
      .join(bDF.withColumn("new_id", monotonically_increasing_id()), "new_id" :: Nil)
    outDF.printSchema()
    TaskOutput(outDF)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")
  val wrapper =
    uhrwerkEnvironment.addTableFile("combiner-C-parq.yml", CombinerCFunc, true)

  val runTimes = Array(
    LocalDateTime.of(2012, 5, 2, 3, 4)
  )
  val results = wrapper.get.runTasksAndWait(runTimes, false)
  println(results)
}
