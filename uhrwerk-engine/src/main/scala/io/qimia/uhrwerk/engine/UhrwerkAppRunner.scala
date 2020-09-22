package io.qimia.uhrwerk.engine

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{Connection, Table}
import io.qimia.uhrwerk.common.tools.TimeTools
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.spark.sql.SparkSession

object UhrwerkAppRunner {

  /**
    * Run Uhrwerk framework as application
    * @param sparkSession spark session required by framemanagers
    * @param environmentConfig location of the environment configuration
    * @param connectionConfigs location of all connection configurations
    * @param tableConfigs location of all table configurations
    * @param runTable identity of exact table that needs to be processed
    * @param startTime starting time partitions inclusive
    * @param endTime end time partitions exclusive
    * @param dagMode run all the dependencies as well (true) or only particular table (false)
    * @param parallelRun run tables in parallel or each
    * @param overwrite
    */
  def runFiles(
      sparkSession: SparkSession,
      environmentConfig: String,
      connectionConfigs: Array[String],
      tableConfigs: Array[String],
      runTable: TableIdent,
      startTime: LocalDateTime,
      endTime: LocalDateTime,
      dagMode: Boolean,
      parallelRun: Int,
      overwrite: Boolean
  ): Unit = {
    // TODO: Perhaps this needs a separate module (to remove the framemanager dependency)
    // It should also support other framemanagers once they're implemented
    val frameManager       = new SparkFrameManager(sparkSession)
    val uhrwerkEnvironment = Environment.build(environmentConfig, frameManager)
    connectionConfigs.foreach(connPath => uhrwerkEnvironment.addConnectionFile(connPath, overwrite))
    val _ = tableConfigs.foreach(tablePath => uhrwerkEnvironment.addTableFileConvention(tablePath, overwrite))
    runEnvironment(uhrwerkEnvironment, runTable, startTime, endTime, dagMode, parallelRun, overwrite)
  }

  /**
    * Run Uhrwerk framework as application
    * @param sparkSession spark session required by framemanagers
    * @param environmentConfig location of the environment configuration
    * @param dagConfig location of the dag configuration
    * @param runTable identity of exact table that needs to be processed
    * @param startTime starting time partitions inclusive
    * @param endTime end time partitions exclusive
    * @param dagMode run all the dependencies as well (true) or only particular table (false)
    * @param parallelRun run tables in parallel or each
    * @param overwrite
    */
  def runDagFile(
      sparkSession: SparkSession,
      environmentConfig: String,
      dagConfig: String,
      runTable: TableIdent,
      startTime: LocalDateTime,
      endTime: LocalDateTime,
      dagMode: Boolean,
      parallelRun: Int,
      overwrite: Boolean
  ): Unit = {
    val frameManager       = new SparkFrameManager(sparkSession)
    val uhrwerkEnvironment = Environment.build(environmentConfig, frameManager)
    uhrwerkEnvironment.setupDagFileConvention(dagConfig, overwrite)
    runEnvironment(uhrwerkEnvironment, runTable, startTime, endTime, dagMode, parallelRun, overwrite)
  }

  /**
    * Run Uhrwerk framework as application
    * @param sparkSession spark session required by framemanagers
    * @param environmentConfig the environment configuration
    * @param connectionConfigs all connection configurations
    * @param tableConfigs all table configurations
    * @param runTable identity of exact table that needs to be processed
    * @param startTime starting time partitions inclusive
    * @param endTime end time partitions exclusive
    * @param dagMode run all the dependencies as well (true) or only particular table (false)
    * @param parallelRun run tables in parallel or each
    * @param overwrite
    */
  def run(
      sparkSession: SparkSession,
      environmentConfig: String,
      connectionConfigs: Array[Connection],
      tableConfigs: Array[Table],
      runTable: TableIdent,
      startTime: LocalDateTime,
      endTime: LocalDateTime,
      dagMode: Boolean,
      parallelRun: Int,
      overwrite: Boolean
  ): Unit = {
    val frameManager       = new SparkFrameManager(sparkSession)
    val uhrwerkEnvironment = Environment.build(environmentConfig, frameManager)
    uhrwerkEnvironment.addConnections(connectionConfigs, overwrite)
    val _ = tableConfigs.foreach(tablePath => uhrwerkEnvironment.addTableConvention(tablePath, overwrite))
    runEnvironment(uhrwerkEnvironment, runTable, startTime, endTime, dagMode, parallelRun, overwrite)
  }

  def runEnvironment(
      environment: Environment,
      runTable: TableIdent,
      startTime: LocalDateTime,
      endTime: LocalDateTime,
      dagMode: Boolean,
      parallelRun: Int,
      overwrite: Boolean
  ): Unit = {
    if (!environment.tables.contains(runTable)) {
      System.err.println("Unknown table to run")
      return
    }
    val tableToRun = environment.getTable(runTable).get
    if (dagMode) {
      val dagTaskBuilder = new DagTaskBuilder(environment)
      val taskList = dagTaskBuilder.buildTaskListFromTable(
        tableToRun,
        startTime,
        endTime
      )
      if (parallelRun > 1) {
        DagTaskDispatcher.runTasksParallel(taskList, parallelRun)
      } else {
        DagTaskDispatcher.runTasks(taskList)
      }
    } else {
      val partitionTs = TimeTools
        .convertRangeToBatch(startTime, endTime, tableToRun.tableDuration)
        .toArray
      if (parallelRun > 1) {
        val _ = tableToRun.runTasksAndWait(partitionTs, overwrite, Option(parallelRun))
      } else {
        val _ = tableToRun.runTasksAndWait(partitionTs, overwrite)
      }
    }
  }

}
