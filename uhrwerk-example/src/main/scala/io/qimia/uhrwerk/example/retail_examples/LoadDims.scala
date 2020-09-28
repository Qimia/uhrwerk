package io.qimia.uhrwerk.example.retail_examples

import io.qimia.uhrwerk.engine.Environment.SourceIdent
import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, hash}

object LoadDims extends App {
  private val logger: Logger = Logger.getLogger(this.getClass)

  val sparkSess = SparkSession
    .builder()
    .appName("loadDims")
    .master("local[*]")
    .config("driver-memory", "6g")
    .getOrCreate()

  def simpleHashLoad(ident: SourceIdent, colName: String): TaskInput => TaskOutput = {
    def udf(in: TaskInput): TaskOutput = {
      in.loadedInputFrames.get(ident) match {
        case Some(df) => TaskOutput(df.withColumn(colName, hash(df.columns.map(col): _*)))
        case None => throw new Exception(s"Table ${ident.toString} not found!")
      }
    }

    udf
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")

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

  val prodResult = prodWrapper.get.runTasksAndWait()
  val employeeResult = employeeWrapper.get.runTasksAndWait()
  val storeResult = storeWrapper.get.runTasksAndWait()

  logger.info(s"Product Dimension processed: $prodResult")
  logger.info(s"Employee Dimension processed: $employeeResult")
  logger.info(s"Store Dimension processed: $storeResult")

}
