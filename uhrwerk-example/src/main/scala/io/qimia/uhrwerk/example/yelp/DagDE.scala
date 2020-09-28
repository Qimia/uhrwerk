package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.yelp.DownsampleE.loaderEFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

object DagDE extends App {
  val sparkSess = SparkSession.builder()
    .appName(this.getClass.toString)
    .master("local[3]")
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")

  val wrapperD = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_d/table_d_1.0.yml", loaderEFunc, overwrite = true).get
  val wrapperE = uhrwerkEnvironment.addTableFile("yelp_test/combining/yelp_db/table_e/table_e_1.0.yml", loaderEFunc, overwrite = true).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperE,
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 7, 0, 0)
  )
  DagTaskDispatcher.runTasks(taskList)

}
