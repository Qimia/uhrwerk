package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.common.metastore.model.{ConnectionModel, TableModel}
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.dag.{DagTaskBuilder, DagTaskDispatcher}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

object UhrwerkAppRunner {
  private val logger: Logger = Logger.getLogger(this.getClass)

  /** Run Uhrwerk framework as application
    * @param sparkSession spark session required by framemanagers
    * @param environmentConfig location of the environment configuration
    * @param connectionConfigs location of all connection configurations
    * @param tableConfigs location of all table configurations
    * @param runTable identity of exact table that needs to be processed
    * @param dagMode run all the dependencies as well (true) or only particular table (false)
    * @param parallelRun run tables in parallel or each
    * @param overwrite
    */
  def runFiles(
      sparkSession: SparkSession,
      environmentConfig: String,
      runTable: TableIdent,
      dagMode: Boolean,
      parallelRun: Int,
      overwrite: Boolean,
      jobProperties: String = null
  ): Unit = {
    val frameManager = new SparkFrameManager(sparkSession)
    val uhrwerkEnvironment =
      Environment.build(environmentConfig, frameManager, jobProperties)

    runEnvironment(
      uhrwerkEnvironment,
      runTable,
      dagMode,
      parallelRun,
      overwrite
    )
  }

  /** Run Uhrwerk framework as application
    * @param sparkSession spark session required by framemanagers
    * @param environmentConfig location of the environment configuration
    * @param dagConfig location of the dag configuration
    * @param runTables Array of identities of exact tables that need to be processed
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
      overwrite: Boolean,
      jobProperties: String = null
  ): Unit = {
    val frameManager = new SparkFrameManager(sparkSession)
    val uhrwerkEnvironment =
      Environment.build(environmentConfig, frameManager, jobProperties)
    uhrwerkEnvironment.setupDagFileConvention(dagConfig, overwrite)
    runTables.foreach(tab =>
      runEnvironment(
        uhrwerkEnvironment,
        tab,
        dagMode,
        parallelRun,
        overwrite
      )
    )
  }

  /** Run Uhrwerk framework as application
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
      overwrite: Boolean,
      jobProperties: String = null
  ): Unit = {
    val frameManager = new SparkFrameManager(sparkSession)
    val uhrwerkEnvironment =
      Environment.build(environmentConfig, frameManager, jobProperties)
    uhrwerkEnvironment.addConnections(connectionConfigs, overwrite)
    val _ = tableConfigs.foreach(tablePath =>
      uhrwerkEnvironment.addTableConvention(tablePath, overwrite)
    )
    runEnvironment(
      uhrwerkEnvironment,
      runTable,
      dagMode,
      parallelRun,
      overwrite
    )
  }

  /** Run a pre-loaded environment for a particular table
    * @param environment environment with loaded table (s)
    * @param runTable table that needs to be processed
    * @param dagMode if activated, this will also process all dependencies using dag-execution (requires all tables to be loaded in environment)
    * @param parallelRun number of threads used by either the dag-execution or the table-wrapper
    * @param overwrite allow overwriting of configs and partitions in the store
    */
  def runEnvironment(
      environment: Environment,
      runTable: TableIdent,
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
      val dagTaskBuilder = new DagTaskBuilder(environment)
      val taskList = dagTaskBuilder.buildTaskListFromTable(tableToRun)

      def identToString(tbl: TableModel) =
        s"${tbl.getArea}.${tbl.getVertical}.${tbl.getName}:${tbl.getVersion}"

      logger.info(
        s"##### RUNNING DAG WITH TARGET TABLE ${identToString(tableToRun.wrappedTable)} #####"
      )

      if (taskList.size >= 1) {
        logger.info(
          s"##### PROCESSING ORDER BEGIN #####"
        )

        taskList.foreach(task => {
          logger.info(
            s"++ ${identToString(task.table.wrappedTable)} ++"
          )
        })

        logger.info(
          s"##### PROCESSING ORDER END  #####"
        )
      }

      if (parallelRun > 1) {
        DagTaskDispatcher.runTasksParallel(taskList, parallelRun)
      } else {
        DagTaskDispatcher.runTasks(taskList)
      }
    } else {
      val partitionTs =
        List.empty[LocalDateTime]
      if (parallelRun > 1) {
        val _ = tableToRun.runTasksAndWait(
          partitionTs,
          overwrite,
          Option(parallelRun)
        )
      } else {
        val _ = tableToRun.runTasksAndWait(partitionTs, overwrite)
      }
    }
  }

}
