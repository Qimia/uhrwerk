package io.qimia.uhrwerk.example.retail_examples

import io.qimia.uhrwerk.engine.{Environment, TaskInput, TaskOutput}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DWHDims extends App {
  val sparkSess = SparkSession
    .builder()
    .appName("DWHDims")
    .master("local[*]")
    .config("driver-memory", "6g")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)

  def dwh(input: TaskInput): TaskOutput = {
    TaskOutput(input.loadedInputFrames.head._2)
  }

  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment = Environment.build("testing-env-config.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("testing-connection-config.yml")

  val prodWrapper = uhrwerkEnvironment.addTableFile(
    "retail_examples/dwh/retail/productDim_1.0.yml",
    dwh
  )
  val employeeWrapper = uhrwerkEnvironment.addTableFile(
    "retail_examples/dwh/retail/employeeDim_1.0.yml",
    dwh
  )
  val storeWrapper = uhrwerkEnvironment.addTableFile(
    "retail_examples/dwh/retail/storeDim_1.0.yml",
    dwh
  )

  val prodResult = prodWrapper.get.runTasksAndWait()
  val employeeResult = employeeWrapper.get.runTasksAndWait()
  val storeResult = storeWrapper.get.runTasksAndWait()

  println(s"Product Dimension processed: ${prodResult}")
  println(s"Employee Dimension processed: ${employeeResult}")
  println(s"Store Dimension processed: ${storeResult}")

}
