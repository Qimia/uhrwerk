package io.qimia.uhrwerk.example.retail

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.example.retail.LoaderSales.loaderAFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}

object WindowSales extends App {

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerC")
    .master("local[3]")
    .getOrCreate()

  def WindowCFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    val sales = in.loadedInputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("sales")).get._2.as("sales")

    val sales_wind = sales.agg(min("selling_date"), max("selling_date"))
    sales_wind.show(10)
    TaskOutput(sales_wind)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")
  val wrapperSales =
    uhrwerkEnvironment.addTableFile("LoadTableSalesTest.yml", loaderAFunc, overwrite = true).get
  val wrapperWindow =
    uhrwerkEnvironment.addTableFile("windowSales.yml", WindowCFunc, overwrite = true).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperWindow,
    LocalDateTime.of(2020, 6, 2, 0, 0),
    LocalDateTime.of(2020, 6, 5, 0, 0)
  )
  DagTaskDispatcher.runTasksParallel(taskList, 2)

}
