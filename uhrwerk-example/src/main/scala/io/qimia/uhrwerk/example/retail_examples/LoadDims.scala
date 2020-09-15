package io.qimia.uhrwerk.example.retail_examples

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.Environment.{SourceIdent, TableIdent}
import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, hash}

object LoadDims extends App {
  val sparkSess = SparkSession.builder().appName("loadDims").master("local").getOrCreate()

  def simpleLoad(ident: SourceIdent): (TaskInput => DataFrame) = {
    def udf(in: TaskInput): DataFrame = {
      in.inputFrames.get(ident) match {
        case Some(x) => x
        case None => throw new Exception(s"Table ${ident.toString} not found!")
      }
    }
    udf
  }

  def simpleHashLoad(ident: SourceIdent, colName: String): (TaskInput => DataFrame) = {
    def udf(in: TaskInput): DataFrame = {
      in.inputFrames.get(ident) match {
        case Some(df) => df.withColumn(colName, hash(df.columns.map(col):_*))
        case None => throw new Exception(s"Table ${ident.toString} not found!")
      }
    }
    udf
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")

  val prodWrapper = uhrwerkEnvironment.addTable("retail_examples/productDim.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.products", "jdbc"), "productKey"))
  val employeeWrapper = uhrwerkEnvironment.addTable("retail_examples/employeeDim.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.employees", "jdbc"), "employeeKey"))
  val storeWrapper = uhrwerkEnvironment.addTable("retail_examples/storeDim.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.stores", "jdbc"), "storeKey"))

  val runTimes = Array(
    LocalDateTime.of(2020,6,1,0,0),
    LocalDateTime.of(2020,6,2,0,0),
    LocalDateTime.of(2020,6,1,0,0)
  )

  val prodResult = prodWrapper.get.runTasksAndWait(runTimes)
  val employeeResult = employeeWrapper.get.runTasksAndWait(runTimes)
  val storeResult = storeWrapper.get.runTasksAndWait(runTimes)


  println(s"Product Dimension processed: ${prodResult}")
  println(s"Employee Dimension processed: ${employeeResult}")
  println(s"Store Dimension processed: ${storeResult}")

}
