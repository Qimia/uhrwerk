package io.qimia.uhrwerk.engine

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{ConnectionModel, TableModel}
import io.qimia.uhrwerk.common.tools.TimeTools
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object UhrwerkAppRunner {
  private val logger: Logger = Logger.getLogger(this.getClass)

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
      startTime: Option[LocalDateTime],
      endTime:Option[LocalDateTime],
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
    * @param runTables Array of identities of exact tables that need to be processed
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
                  runTables: Array[TableIdent],
                  startTime: Option[LocalDateTime],
                  endTime: Option[LocalDateTime],
                  dagMode: Boolean,
                  parallelRun: Int,
                  overwrite: Boolean
                ): Unit = {
    val frameManager       = new SparkFrameManager(sparkSession)
    val uhrwerkEnvironment = Environment.build(environmentConfig, frameManager)
    uhrwerkEnvironment.setupDagFileConvention(dagConfig, overwrite)
    runTables.foreach(tab => runEnvironment(uhrwerkEnvironment, tab, startTime, endTime, dagMode, parallelRun, overwrite))
  }

  /**
    * Run Uhrwerk framework as application
    *
    * @param sparkSession      spark session required by framemanagers
    * @param environmentConfig the environment configuration
    * @param connectionConfigs all connection configurations
    * @param tableConfigs      all table configurations
    * @param runTable          identity of exact table that needs to be processed
    * @param startTime         starting time partitions inclusive
    * @param endTime           end time partitions exclusive
    * @param dagMode run all the dependencies as well (true) or only particular table (false)
    * @param parallelRun run tables in parallel or each
    * @param overwrite
    */
  def run(
           sparkSession: SparkSession,
           environmentConfig: String,
           connectionConfigs: Array[ConnectionModel],
           tableConfigs: Array[TableModel],
           runTable: TableIdent,
           startTime: Option[LocalDateTime],
           endTime: Option[LocalDateTime],
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

  /**
   * fill in Option values for start end time
   * (Only do this when it isn't an unpartitioned table)
   * Uses getTimeLatestPartition for startTime if needed (and stops if it doesn't find anything at all)
   * Uses lowerBoundOfCurrentTimestamp for endTime if needed
   * @param table TableWrapper for the table that needs to run or is targeted by the dag-execution
   * @param startTime option of start-time
   * @param endTime option of end-time
   * @return tuple with start and end-time
   */
  private def getStartEndTime(table: TableWrapper, startTime: Option[LocalDateTime],
                              endTime: Option[LocalDateTime]): (LocalDateTime, LocalDateTime) = {
    val callStartTime = if (startTime.isEmpty) {
      val lastPartitionTime = table.getTimeLatestPartition
      if (lastPartitionTime.isEmpty) {
        logger.error("Calling without start time only functions when partitions have been processed before")
        throw new RuntimeException("Couldn't find latest partition")
      } else {
        lastPartitionTime.get
      }
    } else {
      startTime.get
    }
    val callEndTime = if (endTime.isEmpty) {
      val currentTime = LocalDateTime.now()
      TimeTools.lowerBoundOfCurrentTimestamp(callStartTime, currentTime, table.wrappedTable.getPartitionUnit, table.wrappedTable.getPartitionSize)
    } else {
      endTime.get
    }
    (callStartTime, callEndTime)
  }

  /**
   * Run a pre-loaded environment for a particular table
   * @param environment environment with loaded table (s)
   * @param runTable table that needs to be processed
   * @param startTime possible starting point of partitions to process
   * @param endTime possible ending point of partitions to process (exclusive = first start timestamp of partition not to process)
   * @param dagMode if activated, this will also process all dependencies using dag-execution (requires all tables to be loaded in environment)
   * @param parallelRun number of threads used by either the dag-execution or the table-wrapper
   * @param overwrite allow overwriting of configs and partitions in the store
   */
  def runEnvironment(
      environment: Environment,
      runTable: TableIdent,
      startTime: Option[LocalDateTime],
      endTime: Option[LocalDateTime],
      dagMode: Boolean,
      parallelRun: Int,
      overwrite: Boolean
  ): Unit = {
    if (!environment.tables.contains(runTable)) {
      logger.error("Unknown table to run: " + runTable.toString)
      return
    }
    val tableToRun = environment.getTable(runTable).get

    if (dagMode) {
      val (callStartTime, callEndTime) = getStartEndTime(tableToRun, startTime, endTime)
      val dagTaskBuilder = new DagTaskBuilder(environment)
      val taskList = dagTaskBuilder.buildTaskListFromTable(
        tableToRun,
        callStartTime,
        callEndTime
      )
      if (parallelRun > 1) {
        DagTaskDispatcher.runTasksParallel(taskList, parallelRun)
      } else {
        DagTaskDispatcher.runTasks(taskList)
      }
    } else {
      val partitionTs = if (tableToRun.wrappedTable.isPartitioned) {
        val (callStartTime, callEndTime) = getStartEndTime(tableToRun, startTime, endTime)
        TimeTools
          .convertRangeToBatch(callStartTime, callEndTime, tableToRun.tableDuration)
      } else {
        List.empty[LocalDateTime]
      }
      if (parallelRun > 1) {
        val _ = tableToRun.runTasksAndWait(partitionTs, overwrite, Option(parallelRun))
      } else {
        val _ = tableToRun.runTasksAndWait(partitionTs, overwrite)
      }
    }
  }

}
