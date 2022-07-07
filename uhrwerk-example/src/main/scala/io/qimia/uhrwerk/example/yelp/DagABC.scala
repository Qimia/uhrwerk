package io.qimia.uhrwerk.example.yelp

import java.time.LocalDateTime
import io.qimia.uhrwerk.engine.Environment
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.example.yelp.CombinerC.CombinerCFunc
import io.qimia.uhrwerk.example.yelp.LoaderAParq.loaderAFunc
import io.qimia.uhrwerk.example.yelp.LoaderBParq.loaderBFunc
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

import java.nio.file.Files

object DagABC extends App {
  val tmpDir = Files.createTempDirectory("spark-events")

  val sparkSess = SparkSession.builder()
    .appName(this.getClass.toString)
    .master("local[3]")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("driver-memory", "2g")
    .config("spark.eventLog.dir", tmpDir.toAbsolutePath.toString)
    .getOrCreate()
  val frameManager = new SparkFrameManager(sparkSess)

  val uhrwerkEnvironment =
    Environment.build("yelp_test/uhrwerk.yml", frameManager)
  uhrwerkEnvironment.addConnectionFile("yelp_test/testing-connection-config.yml")

  val wrapperA = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_a_parq/table_a_parq_1.0.yml", loaderAFunc, overwrite = false).get
  val wrapperB = uhrwerkEnvironment.addTableFile("yelp_test/staging/yelp_db/table_b_parq/table_b_parq_1.0.yml", loaderBFunc, overwrite = false).get
  val wrapperC = uhrwerkEnvironment.addTableFile("yelp_test/combining/yelp_db/table_c_parq/table_c_parq_1.0.yml", CombinerCFunc, overwrite = false).get

  val dagTaskBuilder = new DagTaskBuilder(uhrwerkEnvironment)
  val taskList = dagTaskBuilder.buildTaskListFromTable(
    wrapperC,
    LocalDateTime.of(2012, 5, 1, 0, 0),
    LocalDateTime.of(2012, 5, 6, 0, 0)
  )
  DagTaskDispatcher.runTasksParallel(taskList, 2)
}
