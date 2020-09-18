package io.qimia.uhrwerk.example.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.Environment.SourceIdent
import io.qimia.uhrwerk.engine.dag.{DagTask, DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.retail_examples.LoadDims.simpleHashLoad
import io.qimia.uhrwerk.example.retail_examples.LoadFacts.{computeFactTable, computeWeeklyFacts, simpleLoad}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RetailDAG extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val numberOfCores = Runtime.getRuntime.availableProcessors

  val sparkSess = SparkSession
    .builder()
    .appName(this.getClass.toString)
    .master(s"local[${numberOfCores - 1}]")
    .config("driver-memory", "4g")
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")

  val prodWrapper = uhrwerkEnvironment.addTableFile(
    "retail_examples/staging/retail/productDim_1.0.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.products", "jdbc"), "productKey")
  )
  val employeeWrapper = uhrwerkEnvironment.addTableFile(
    "retail_examples/staging/retail/employeeDim_1.0.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.employees", "jdbc"), "employeeKey")
  )
  val storeWrapper = uhrwerkEnvironment.addTableFile(
    "retail_examples/staging/retail/storeDim_1.0.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.stores", "jdbc"), "storeKey")
  )

  val salesWrapper = uhrwerkEnvironment.addTableFile(
    "retail_examples/staging/retail/salesFact_1.0.yml",
    simpleLoad(SourceIdent("retail_mysql", "qimia_oltp.sales_items", "jdbc"))
  )
  val salesFactWrapper =
    uhrwerkEnvironment.addTableFile("retail_examples/dwh/retail/salesFact_1.0.yml", computeFactTable)
  val salesFactDailyWrapper =
    uhrwerkEnvironment.addTableFile("retail_examples/dwh/retail/salesFactsDaily_1.0.yml", computeWeeklyFacts)

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList: List[DagTask] = dagTaskBuilder.buildTaskListFromTable(
    salesFactDailyWrapper.get,
    LocalDateTime.of(2020, 6, 1, 0, 0),
    LocalDateTime.of(2020, 6, 7, 0, 0)
  )

  println("task list size: " + taskList.size)
  taskList.foreach(t => println(s"${t.table.wrappedTable.getArea}/${t.table.wrappedTable.getVertical}/${t.table.wrappedTable.getName} - partitions: ${t.partitions.toString()}"))
  DagTaskDispatcher.runTasksParallel(taskList, 2)
}
