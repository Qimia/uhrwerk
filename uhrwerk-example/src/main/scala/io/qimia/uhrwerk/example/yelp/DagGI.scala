package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.yelp.CombinerI.transformationFunction
import io.qimia.uhrwerk.example.yelp.LoaderUnpartitionedG.loaderUnpartitionedGFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

object DagGI extends App {
  val sparkSess = SparkSession.builder()
    .appName(this.getClass.toString)
    .master("local[3]")
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")

  val wrapperG = uhrwerkEnvironment.addTableFile("loader-unpartitioned-G.yml", loaderUnpartitionedGFunc).get
  val wrapperI = uhrwerkEnvironment.addTableFile("combiner-I.yml", transformationFunction).get

  val now = LocalDateTime.now()
  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperI,
    now,
    now
  )
  DagTaskDispatcher.runTasks(taskList)
}
