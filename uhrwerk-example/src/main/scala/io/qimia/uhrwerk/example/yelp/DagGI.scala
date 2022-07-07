package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.yelp.CombinerI.transformationFunction
import io.qimia.uhrwerk.example.yelp.LoaderUnpartitionedG.loaderUnpartitionedGFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

object DagGI extends App {
  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession.builder()
    .appName(this.getClass.toString)
    .master("local[3]")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")

  val wrapperG = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_g/table_g_1.0.yml", loaderUnpartitionedGFunc).get
  val wrapperI = uhrwerkEnvironment.addTableFile("yelp_test/combining/yelp_db/table_i/table_i_1.0.yml", transformationFunction).get

  val now = LocalDateTime.now()
  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperI,
    now,
    now
  )
  DagTaskDispatcher.runTasks(taskList)
}
