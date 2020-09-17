package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.yelp.CombinerC.CombinerCFunc
import io.qimia.uhrwerk.example.yelp.LoaderAParq.loaderAFunc
import io.qimia.uhrwerk.example.yelp.LoaderBParq.loaderBFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DagABC extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val sparkSess = SparkSession.builder()
    .appName(this.getClass.toString)
    .master("local[3]")
//    .config("spark.eventLog.enabled", true)
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")

  val wrapperA = uhrwerkEnvironment.addTableFile("loader-A-parq.yml", loaderAFunc, false).get
  val wrapperB = uhrwerkEnvironment.addTableFile("loader-B-parq.yml", loaderBFunc, false).get
  val wrapperC = uhrwerkEnvironment.addTableFile("combiner-C-parq.yml", CombinerCFunc, false).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperC,
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 6, 0, 0)
  )
  DagTaskDispatcher.runTasksParallel(taskList, 2)
}
