package io.qimia.uhrwerk.example.retail

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.example.retail.LoaderSales.loaderAFunc
import io.qimia.uhrwerk.example.retail.LoaderStores.loaderBFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

object CombineSalesStores extends App {

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerC")
    .master("local[3]")
    .getOrCreate()

  def CombinerCFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    val sales = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("sales")).get._2.as("sales")
      .withColumnRenamed("store","store_id")
    val stores = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("stores")).get._2.as("stores")
    TaskOutput(sales.join(stores, "store_id" :: Nil))

  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapperStore =
    uhrwerkEnvironment.addTableFile("LoadTableStoresTest.yml", loaderBFunc, overwrite = true).get
  val wrapperSales =
    uhrwerkEnvironment.addTableFile("LoadTableSalesTest.yml", loaderAFunc, overwrite = true).get
  val wrapperCombine =
    uhrwerkEnvironment.addTableFile("combineSalesStores.yml", CombinerCFunc, overwrite = true).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperCombine,
    LocalDateTime.of(2020, 6, 1, 0, 0),
    LocalDateTime.of(2020, 6, 6, 0, 0)
  )
  DagTaskDispatcher.runTasksParallel(taskList, 2)

}
