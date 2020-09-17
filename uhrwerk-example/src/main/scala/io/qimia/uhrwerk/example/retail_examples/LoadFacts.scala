package io.qimia.uhrwerk.example.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.{SourceIdent, TableIdent}
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, sum}

object LoadFacts extends App {
  val sparkSess = SparkSession.builder()
    .appName("loadDims")
    .master("local[*]")
    .config("driver-memory", "4g")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def simpleLoad(ident: SourceIdent): (TaskInput => TaskOutput) = {
    def udf(in: TaskInput): TaskOutput = {
      in.loadedInputFrames.get(ident) match {
        case Some(x) => TaskOutput(x)
        case None => throw new Exception(s"Table ${ident.toString} not found!")
      }
    }

    udf
  }

  def computeFactTable(in: TaskInput): TaskOutput = {
    val facts = in.loadedInputFrames.get(TableIdent("staging", "retail", "salesFacts", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table salesFact not found!")
    }
    val employeeDim = in.loadedInputFrames.get(TableIdent("staging", "retail", "employeeDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table employeeDim not found!")
    }
    val storeDim = in.loadedInputFrames.get(TableIdent("staging", "retail", "storeDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table storeDim not found!")
    }
    val storeEmployees = in.loadedInputFrames.get(SourceIdent("retail_mysql", "qimia_oltp.stores_employees", "jdbc")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table storeEmployees not found")
    }
    val productDim = in.loadedInputFrames.get(TableIdent("staging", "retail", "productDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table productDim not found!")
    }

    val pFacts = facts.as("f").join(productDim.as("p"), col("f.product_id") === col("p.product_id"))
      .select("f.product_id", "f.quantity", "f.sales_id", "f.cashier", "f.store", "f.selling_date", "f.year",
        "f.month", "f.day", "p.productKey")

    val sFacts = pFacts.as("p").join(storeDim.as("s"), col("p.store") === col("s.store_id"))
      .select("p.product_id", "p.quantity", "p.sales_id", "p.cashier", "p.store", "p.selling_date", "p.year",
        "p.month", "p.day", "p.productKey", "s.storeKey")

    val eFacts = sFacts.as("s").join(storeEmployees.as("se"), col("s.store") === col("se.store_id"))
      .join(employeeDim.as("e"), col("se.employee_id") === col("e.employee_id"))
      .select("s.product_id", "s.quantity", "s.sales_id", "s.cashier", "s.store", "s.selling_date", "s.year",
        "s.month", "s.day", "s.productKey", "s.storeKey", "e.employeeKey")

    TaskOutput(eFacts)
  }

  def computeWeeklyFacts(in: TaskInput): TaskOutput = {
    val salesFacts = in.loadedInputFrames.get(TableIdent("dwh", "retail", "salesFact", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception("Table salesFact not found!")
    }

    TaskOutput(salesFacts.drop("employeeKey", "cashier", "store", "year", "month", "day")
      .groupBy("selling_date", "storeKey", "productKey")
      .agg(count("sales_id"), sum("quantity")))
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")

  val salesWrapper = uhrwerkEnvironment.addTable("retail_examples/staging/retail/salesFact_1.0.yml",
    simpleLoad(SourceIdent("retail_mysql", "qimia_oltp.sales_items", "jdbc")))
  val salesFactWrapper = uhrwerkEnvironment.addTable("retail_examples/dwh/retail/salesFact_1.0.yml", computeFactTable)
  val salesFactDailyWrapper = uhrwerkEnvironment.addTable("retail_examples/dwh/retail/salesFactsDaily_1.0.yml", computeWeeklyFacts)

  val runTimes = Array(
    LocalDateTime.of(2020, 6, 1, 0, 0),
    LocalDateTime.of(2020, 6, 2, 0, 0),
    LocalDateTime.of(2020, 6, 3, 0, 0),
  )

  val salesResult = salesWrapper.get.runTasksAndWait(runTimes)
  val salesFactResult = salesFactWrapper.get.runTasksAndWait(runTimes)
  val salesFactDailyResult = salesFactDailyWrapper.get.runTasksAndWait(runTimes)

  println(s"Sales Fact processed: ${salesResult}")
  println(s"Sales Fact processed to dwh: ${salesFactResult}")
  println(s"Sales Fact Daily processed to dwh: ${salesFactDailyResult}")
}
