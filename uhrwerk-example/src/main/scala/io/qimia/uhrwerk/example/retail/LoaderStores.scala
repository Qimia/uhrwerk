package io.qimia.uhrwerk.example.retail

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoaderStores extends App {

  val sparkSess = SparkSession.builder()
    .appName("loaderB")
    .master("local")
    .getOrCreate()

  def loaderBFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    TaskOutput(in.loadedInputFrames.values.head)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml" ,frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")
  val wrapper = uhrwerkEnvironment.addTableFile("LoadTableStoresTest.yml", loaderBFunc, true).get


  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapper,
    LocalDateTime.of(2020, 6, 1, 0, 0),
    LocalDateTime.of(2020, 6, 6, 0, 0)
  )
  taskList.foreach(tup => print(tup))
  DagTaskDispatcher.runTasksParallel(taskList, 2)
}
