package io.qimia.uhrwerk.example.retail_examples

import io.qimia.uhrwerk.engine.Environment.SourceIdent
import io.qimia.uhrwerk.engine.{Environment, TaskInput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, hash}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadDims extends App {
  val sparkSess = SparkSession
    .builder()
    .appName("loadDims")
    .master("local[*]")
    .config("driver-memory", "6g")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def simpleHashLoad(ident: SourceIdent, colName: String): (TaskInput => DataFrame) = {
    def udf(in: TaskInput): DataFrame = {
      in.inputFrames.get(ident) match {
        case Some(df) => df.withColumn(colName, hash(df.columns.map(col): _*))
        case None => throw new Exception(s"Table ${ident.toString} not found!")
      }
    }

    udf
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnections("testing-connection-config.yml")

  val prodWrapper = uhrwerkEnvironment.addTable(
    "retail_examples/productDim.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.products", "jdbc"), "productKey")
  )
  val employeeWrapper = uhrwerkEnvironment.addTable(
    "retail_examples/employeeDim.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.employees", "jdbc"), "employeeKey")
  )
  val storeWrapper = uhrwerkEnvironment.addTable(
    "retail_examples/storeDim.yml",
    simpleHashLoad(SourceIdent("retail_mysql", "qimia_oltp.stores", "jdbc"), "storeKey")
  )

  val prodResult = prodWrapper.get.runTasksAndWait()
  val employeeResult = employeeWrapper.get.runTasksAndWait()
  val storeResult = storeWrapper.get.runTasksAndWait()

  println(s"Product Dimension processed: ${prodResult}")
  println(s"Employee Dimension processed: ${employeeResult}")
  println(s"Store Dimension processed: ${storeResult}")

}
