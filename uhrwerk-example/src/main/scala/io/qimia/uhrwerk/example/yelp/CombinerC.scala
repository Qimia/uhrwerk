package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

import java.nio.file.Files

object CombinerC extends App {

  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerC")
    .master("local[3]")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()

  private val logger: Logger = Logger.getLogger(this.getClass)

  def CombinerCFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    val aDF = in.loadedInputFrames(TableIdent("staging", "yelp_db", "table_a_parq", "1.0"))
      .drop("id", "user_id", "year", "month", "day", "hour", "minute", "date")
    val bDF = in.loadedInputFrames(TableIdent("staging", "yelp_db", "table_b_parq", "1.0"))
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
    Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapper =
    uhrwerkEnvironment.addTableFile("yelp_test/combining/yelp_db/table_c_parq/table_c_parq_1.0.yml", CombinerCFunc, overwrite = true)

  val runTimes = List(
    LocalDateTime.of(2012, 5, 2, 3, 4)
  )
  val results = wrapper.get.runTasksAndWait(runTimes, overwrite = false)
  logger.info(results)
}
