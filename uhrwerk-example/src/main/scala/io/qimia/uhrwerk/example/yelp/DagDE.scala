package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.yelp.DownsampleE.{loaderEFunc, uhrwerkEnvironment}
import io.qimia.uhrwerk.example.yelp.LoaderD.{loaderDFunc, uhrwerkEnvironment}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DagDE extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkSess = SparkSession.builder()
    .appName(this.getClass.toString)
    .master("local[3]")
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")

  val wrapperD = uhrwerkEnvironment.addTableFile("loader-D.yml", loaderEFunc, true).get
  val wrapperE = uhrwerkEnvironment.addTableFile("downsample-E.yml", loaderEFunc, true).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperE,
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 7, 0, 0)
  )
  DagTaskDispatcher.runTasks(taskList)

}
