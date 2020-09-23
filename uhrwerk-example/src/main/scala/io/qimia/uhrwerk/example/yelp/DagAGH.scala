package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.yelp.CombinerH.transformationFunction
import io.qimia.uhrwerk.example.yelp.LoaderAParq.loaderAFunc
import io.qimia.uhrwerk.example.yelp.LoaderUnpartitionedG.loaderUnpartitionedGFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

object DagAGH extends App {
  val sparkSess = SparkSession.builder()
    .appName(this.getClass.toString)
    .master("local[3]")
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")

  val wrapperA = uhrwerkEnvironment.addTableFile("loader-A-parq.yml", loaderAFunc, false).get
  val wrapperG = uhrwerkEnvironment.addTableFile("loader-unpartitioned-G.yml", loaderUnpartitionedGFunc).get
  val wrapperH = uhrwerkEnvironment.addTableFile("combiner-H.yml", transformationFunction).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperH,
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 6, 0, 0)
  )
  DagTaskDispatcher.runTasksParallel(taskList, 2)
}
