package io.qimia.uhrwerk.example.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.{SourceIdent, TableIdent}
import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadFacts extends App {
  val sparkSess = SparkSession.builder()
    .appName("loadDims")
    .master("local[*]")
    .config("driver-memory", "6g")
    .getOrCreate()

  def simpleLoad(ident: SourceIdent): (TaskInput => DataFrame) = {
    def udf(in: TaskInput): DataFrame = {
      in.inputFrames.get(ident) match {
        case Some(x) => x
        case None => throw new Exception(s"Table ${ident.toString} not found!")
      }
    }

    udf
  }

  def computeFactTable(in: TaskInput): DataFrame = {
    val facts = in.inputFrames.get(TableIdent("staging", "retail", "salesFacts", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table salesFact not found!")
    }
    val employeeDim = in.inputFrames.get(TableIdent("staging", "retail", "employeeDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table employeeDim not found!")
    }
    val storeDim = in.inputFrames.get(TableIdent("staging", "retail", "storeDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table storeDim not found!")
    }
    val productDim = in.inputFrames.get(TableIdent("staging", "retail", "productDim", "1.0")) match {
      case Some(x) => x
      case None => throw new Exception(s"Table productDim not found!")
    }

    facts.show(20)
    productDim.show(20)

    val pFacts = facts.as("f").join(productDim.as("p"), col("f.product_id") === col("p.product_id"))
      .select("f.product_id", "f.quantity", "f.sales_id", "f.cashier", "f.store", "f.selling_date", "f.year",
        "f.month", "f.day", "p.productKey")

    pFacts.show(20)

    val sFacts = pFacts.as("p").join(storeDim.as("s"), col("p.store") === col("s.store_id"))
      .select("p.product_id", "p.quantity", "p.sales_id", "p.cashier", "p.store", "p.selling_date", "p.year",
        "p.month", "p.day", "p.productKey", "p.storeKey")

    sFacts
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")

  val salesWrapper = uhrwerkEnvironment.addTable("retail_examples/salesFact.yml",
    simpleLoad(SourceIdent("retail_mysql", "qimia_oltp.sales_items", "jdbc")))
  val salesFactWrapper = uhrwerkEnvironment.addTable("retail_examples/salesFact_dwh.yml", computeFactTable)

  val runTimes = Array(
    LocalDateTime.of(2020, 6, 1, 0, 0),
  )

  val salesResult = salesWrapper.get.runTasksAndWait(runTimes)
  val salesFactResult = salesFactWrapper.get.runTasksAndWait(runTimes)

  println(s"Sales Fact processed: ${salesResult}")
  println(s"Sales Fact processed to dwh: ${salesFactResult}")
}
