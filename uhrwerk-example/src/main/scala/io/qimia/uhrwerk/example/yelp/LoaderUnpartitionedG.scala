package io.qimia.uhrwerk.example.yelp

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object LoaderUnpartitionedG extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val sparkSess = SparkSession.builder()
    .appName("loaderUnpartitionedG")
    .master("local[*]")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("driver-memory", "4g")
    .getOrCreate()

  def loaderUnpartitionedGFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    in.loadedInputFrames.values.head.show()
    TaskOutput(in.loadedInputFrames.values.head)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_g/table_g_1.0.yml", loaderUnpartitionedGFunc)

  val results = wrapper.get.runTasksAndWait()
  logger.info(results)
}
