package io.qimia.uhrwerk.example.retail

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.example.retail.LoaderSales.loaderAFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AggregateSales extends App {

  val sparkSess = SparkSession
    .builder()
    .appName("CombinerC")
    .master("local[3]")
    .getOrCreate()

  def AggCFunc(in: TaskInput): TaskOutput = {
    // The most basic userFunction simply returns the input dataframe
    //val sales = in.inputFrames.find(t => t._1.asInstanceOf[TableIdent].name.equals("sales")).get._2.as("sales")

    //val sales_wind = sales.agg(min("selling_date"), max("selling_date"))
    //sales_wind.show(10)
    //sales_wind

    val aDF = in.loadedInputFrames.values.head
    aDF.select("selling_date").agg(min("selling_date"), max("selling_date")).show()
    aDF.printSchema()
    aDF.show(10)
    TaskOutput(aDF)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")
  val wrapperSales =
    uhrwerkEnvironment.addTableFile("LoadTableSalesTest.yml", loaderAFunc, true).get
  val wrapperAgg =
    uhrwerkEnvironment.addTableFile("aggSales.yml", AggCFunc, true).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperAgg,
    LocalDateTime.of(2020, 6, 2, 0, 0),
    LocalDateTime.of(2020, 6, 6, 0, 0)
  )
  DagTaskDispatcher.runTasksParallel(taskList, 2)

}
