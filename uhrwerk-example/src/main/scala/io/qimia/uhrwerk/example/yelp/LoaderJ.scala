package io.qimia.uhrwerk.example.yelp

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{DependentLoaderSource, Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoaderJ extends App {
  val sparkSess = SparkSession
    .builder()
    .appName("LoaderJ")
    .master("local[*]")
    .config("driver-memory", "2g")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "./docker/spark_logs")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def transformationFunction(in: TaskInput): TaskOutput = {
    val user: DataFrame = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name == "table_g").get._2
    val business: DataFrame = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name == "table_d").get._2

    val filteredUsers = user
      .filter(col("review_count") > 10)

    val filteredBusiness = business
      .filter(col("stars") > 3)

    val reviewLoader: DependentLoaderSource = in.notLoadedSources.find(s => s._1.path == "review").get._2

    reviewLoader.addDependentFrame(filteredUsers, "id", "<user_ids>")

    reviewLoader.addDependentFrame(filteredBusiness, "id", "<business_ids>")

    val review = reviewLoader.getFrame()

    println("Count: " + review.count())

    val dataFrameWriterOptions = Option(Array(Map("partitionBy" -> "user_id,business_id")))

    TaskOutput(review, dataFrameWriterOptions)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("loader-J.yml", transformationFunction)

  val runTimes = Array(LocalDateTime.of(2012, 5, 1, 0, 0))
  val now = LocalDateTime.now()
  val results = wrapper.get.runTasksAndWait(runTimes)
  println(results)
  println("Took " + Duration.between(now, LocalDateTime.now()).getSeconds + " s.")
}
