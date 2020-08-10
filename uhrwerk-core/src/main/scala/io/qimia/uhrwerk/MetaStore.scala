package io.qimia.uhrwerk

import java.nio.file.Path
import java.sql
import java.sql.DriverManager
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.MetaStore.{DependencyFailedOld, DependencySuccess}
import io.qimia.uhrwerk.backend.jpa.{PartitionLog, TaskLog}
import io.qimia.uhrwerk.config.TaskLogType
import io.qimia.uhrwerk.config.dao.config.{ConnectionDAO, TableDAO}
import io.qimia.uhrwerk.config.model._
import io.qimia.uhrwerk.utils.{ConfigProcess, ConfigReader, JDBCTools, TimeTools}
import javax.persistence.{EntityManager, Persistence}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.sys.exit

object MetaStore {
  type DependencyFailedOld = (LocalDateTime, Set[String])
  type DependencySuccess = LocalDateTime
  type TargetNeededOld = (LocalDateTime, Set[String])

  /**
   * Retrieve the latest TaskLog for a particular Table
   *
   * @param store     Persistence Entity manager
   * @param tableName Name of the step
   * @return Latest tasklog if there is one
   */
  def getLastTaskLog(store: EntityManager,
                     tableName: String): Option[TaskLog] = {
    val results = store
      .createQuery(s"FROM TaskLog WHERE tableName = '${tableName}'" +
        "ORDER BY runTs DESC",
        classOf[TaskLog])
      .setMaxResults(1)
      .getResultList
      .asScala
    if (results.isEmpty) {
      Option.empty
    } else {
      Option(results.head)
    }
  }

  // Starttime and endtime inclusive
  def getBatchedDependencies(store: EntityManager,
                             batchedDependencies: Array[Dependency],
                             startTimes: Seq[LocalDateTime],
                             batchDuration: Duration)
  : List[Either[DependencyFailedOld, DependencySuccess]] = {
    // TODO: Create groupBy version when figured out howto filter each dependency with its version number
    // (Could be done with a very big WHERE (a-specs OR b-specs OR c-specs) clause, not sure if there is a performance gain)

    val storedPartitions: mutable.Map[LocalDateTime, Set[String]] =
      new mutable.HashMap().withDefaultValue(Set())

    // Either we only send a start and enddate or we have to send all dates in the query
    if (TimeTools.checkIsSequentialIncreasing(startTimes, batchDuration)) {
      val startTime = startTimes.head
      val endTime = startTimes.last

      def updateStoredPartitions(dep: Dependency): Unit = {
        val depPathName = dep.getPath // TODO: If Dependencies gets a proper name / key field use that instead
        val results = store
          .createQuery(
            s"FROM PartitionLog WHERE area = '${dep.getArea}' " +
              s"AND vertical = '${dep.getVertical}' " +
              s"AND path = '${depPathName}' " +
              s"AND version = ${dep.getVersion} " +
              s"AND partitionDuration = :partDur " +
              s"AND partitionTs BETWEEN :partstart AND :partfinish",
            // TODO: Need to check connection
            classOf[PartitionLog]
          )
          .setParameter("partDur", dep.getPartitionSizeDuration)
          .setParameter("partstart", startTime)
          .setParameter("partfinish", endTime)
          .getResultList
          .asScala
        results
          .map(_.getPartitionTs)
          .foreach(ldt => storedPartitions(ldt) += depPathName)
      }

      batchedDependencies.foreach(updateStoredPartitions)
    } else {
      def updateStoredPartitions(dep: Dependency): Unit = {
        val depPathName = dep.getPath // TODO: If Dependencies gets a proper name / key field use that instead
        val results = store
          .createQuery(
            s"FROM PartitionLog WHERE area = '${dep.getArea}' " +
              s"AND vertical = '${dep.getVertical}' " +
              s"AND path = '${depPathName}' " +
              s"AND version = ${dep.getVersion} " +
              s"AND partitionDuration = :partDur " +
              s"AND partitionTs IN (:partTimes)",
            // TODO: Need to check connection
            classOf[PartitionLog]
          )
          .setParameter("partDur", dep.getPartitionSizeDuration)
          .setParameter("partTimes", startTimes.asJava)
          .getResultList
          .asScala
        results
          .map(_.getPartitionTs)
          .foreach(ldt => storedPartitions(ldt) += depPathName)
      }

      batchedDependencies.foreach(updateStoredPartitions)
    }

    // Translate the resulting dependencies found to if there are any still needed or not
    val requiredDependencies = batchedDependencies.map(_.getPath).toSet
    startTimes
      .map(startTime => {
        val partitionsAvailable = storedPartitions.get(startTime)
        if (partitionsAvailable.isEmpty) {
          Left(startTime -> requiredDependencies)
        } else {
          val missingPart = requiredDependencies diff partitionsAvailable.get
          if (missingPart.isEmpty) {
            Right(startTime)
          } else {
            Left(startTime -> missingPart)
          }
        }
      })
      .toList
  }

  def getUnprocessedTargets(store: EntityManager,
                            table: Table,
                            startTimes: Seq[LocalDateTime]): List[TargetNeededOld] = {
    val storedPartitions: mutable.Map[LocalDateTime, Set[String]] =
      new mutable.HashMap().withDefaultValue(Set())
    val targets = table.getTargets

    val results = store
      .createQuery(
        s"FROM PartitionLog WHERE area = '${table.getTargetArea}' " +
          s"AND vertical = '${table.getTargetVertical}' " +
          s"AND path in (:pathNames) " +
          s"AND version = ${table.getTargetVersion} " +
          s"AND partitionDuration = :partDur " +
          s"AND partitionTs IN (:partTimes)",
        classOf[PartitionLog]
      )
      .setParameter("pathNames", targets.map(_.getPath))
      .setParameter("partDur", table.getTargetPartitionSizeDuration)
      .setParameter("partTimes", startTimes.asJava)
      .getResultList
      .asScala
    results
      .foreach(pl => storedPartitions(pl.getPartitionTs) += pl.getPath)

    val targetSet = targets.toSet
    val targetPathSet = targetSet.map(t => t.getPath)

    val includingEmptyRes: Seq[TargetNeededOld] = startTimes.map(time =>
      if (!storedPartitions.contains(time)) {
        // Every target is needed
        (time, targetPathSet)
      } else {
        val partitionPaths = storedPartitions(time)
        if (partitionPaths.size == targets.size) {
          val emptySet: Set[String] = Set()
          // Don't need to run and can be filtered out
          (time, emptySet)
        } else {
          // only targets not found are needed
          (time, targetPathSet diff partitionPaths)
        }
      })
    includingEmptyRes.filter(x => x._2.nonEmpty).toList
  }

  /**
   * Creates an SQL Connection from a global config.
   * This config needs to contain a connection with the name specified in ConfigProcess.UHRWERK_BACKEND_CONNECTION_NAME.
   * If it doesn't, the function will fail.
   *
   * @param globalConf Global Config
   * @return SQL Connection
   */
  def createSqlConnectionFromConfig(globalConf: Global): java.sql.Connection = {
    val backendConnection = globalConf
      .getConnections
      .find(c => c.getName.equals(ConfigProcess.UHRWERK_BACKEND_CONNECTION_NAME))
      .get

    println(backendConnection)
    JDBCTools.executeSqlFile(backendConnection, ConfigProcess.UHRWERK_BACKEND_SQL_CREATE_TABLES_FILE)
    JDBCTools.getJDBCConnection(backendConnection)
  }

  def apply(globalConf: Path,
            tableConf: Path,
            validate: Boolean = true): MetaStore = {
    val globalConfig: Global = ConfigReader.readGlobalConfig(globalConf)
    val tableConfig: Table = ConfigReader.readStepConfig(tableConf)
    new MetaStore(globalConfig, tableConfig, validate)
  }
}

class MetaStore(globalConf: Global,
                tableConf: Table,
                validate: Boolean = true) {
  // TODO: After loading the config we should check if it is correct (if it's in a valid state)
  if (validate) {
    val valid = ConfigProcess.enrichAndValidateConfig(tableConf, globalConf)
    if (!valid) {
      System.err.println("loaded configuration is not valid")
      exit(1)
    }
  }

  // save the configs to the backend
  val backendDb: sql.Connection = MetaStore.createSqlConnectionFromConfig(globalConf)
  globalConf.getConnections.foreach(c => ConnectionDAO.save(backendDb, c))
  TableDAO.save(backendDb, tableConf)

  val globalConfig: Global = globalConf
  val tableConfig: Table = tableConf
  val connections: Map[String, Connection] = globalConfig.getConnections.map(conn => conn.getName -> conn).toMap

  // todo remove
  val storeFactory =
    Persistence.createEntityManagerFactory("io.qimia.uhrwerk.backend.jpa")

  // For each LocalDateTime check if all dependencies are there or tell which ones are not
  /**
   * Check for an array of dependencies (with the same partition size) and a sequence of partitionTimeStamps if
   * all the dependencies have been met or which ones have not been met. This does not translate the timestamps based
   * on dependency type, meaning the caller has to already have processed those (see TableWrapper's virtual dependencies)
   *
   * @param batchedDependencies array of Dependencies
   * @param startTimes          timestamps for which the dependencies will be tested
   * @return DependencyFailed or DependencySuccess for each of the starttimes
   */
  def checkDependencies(batchedDependencies: Array[Dependency],
                        startTimes: Seq[LocalDateTime])
  : List[Either[DependencyFailedOld, DependencySuccess]] = {
    val partitionSizeSet =
      batchedDependencies.map(_.getPartitionSizeDuration).toSet
    require(partitionSizeSet.size == 1)
    // We only allow the same batchSize as input and it can only be a simple check if the datetime is there
    val store = storeFactory.createEntityManager
    val ta = store.getTransaction
    ta.begin()
    val res =
      MetaStore.getBatchedDependencies(store,
        batchedDependencies,
        startTimes,
        partitionSizeSet.head)
    ta.commit()
    store.close()
    res
  }

  /**
   * Flag the start of a task. The Metastore will log this and return the log object
   * (used when writing the matching finish object)
   *
   * @return TaskLog object stored using persistence
   */
  def logStartTask(): TaskLog = {
    // Make sure that getting the latest runNr and incrementing it is done
    // in 1 transaction
    val store = storeFactory.createEntityManager

    val ta = store.getTransaction
    ta.begin
    val previousRun = MetaStore.getLastTaskLog(store, tableConfig.getName)
    // TODO: For now version without storing the config (but should be configurable / devmode)
    val previousRunNr = if (previousRun.isDefined) {
      previousRun.get.getRunNumber + 1
    } else {
      1
    }
    val startLog = new TaskLog(
      tableConfig.getName,
      previousRunNr, // How often did this table run
      tableConfig.getTargetVersion,
      LocalDateTime.now(),
      Duration.ZERO,
      TaskLogType.START
    )

    store.persist(startLog)
    ta.commit()
    store.close()
    // TODO: Transaction failure??
    startLog
  }

  /**
   * Flag the end of task. The metastore will write the finished log plus the partition logs if the task was completed
   * successfully.
   *
   * @param startLog    Matching start-log generated by [[io.qimia.uhrwerk.MetaStore#logStartTask()]]
   * @param partitionTS Starttime of the partition that ran
   * @param success     If the task was a success or not (most often was the partition successfully written)
   */
  def logFinishTask(startLog: TaskLog,
                    partitionTS: LocalDateTime,
                    success: Boolean) = {
    val store = storeFactory.createEntityManager

    // First write finish log based on success or failure
    val ta = store.getTransaction
    ta.begin
    val timeNow = LocalDateTime.now()
    val logType = if (success) {
      TaskLogType.SUCCESS
    } else {
      TaskLogType.FAILURE
    }
    val finishLog = new TaskLog(
      tableConfig.getName,
      startLog.getRunNumber(),
      tableConfig.getTargetVersion,
      timeNow,
      Duration.between(startLog.getRunTs, timeNow),
      logType
    )
    store.persist(finishLog)

    // if success then also call writePartitionLog next
    // Either fails completely or writes all tasklogs (no partial completion)
    if (success) {
      tableConfig.getTargets.foreach(t =>
        this.writePartitionLog(store, finishLog, t, tableConfig, partitionTS))
    }
    ta.commit()
    store.close()
  }

  /**
   * Write a partitionlog for a particular target for one partition timestamp
   *
   * @param store       Persistence Entity manager
   * @param finishLog   reference to the persisted finished TaskLog
   * @param target      the target for which a partition was written
   * @param partitionTS the starting timestamp of the partition
   */
  def writePartitionLog(store: EntityManager,
                        finishLog: TaskLog,
                        target: Target,
                        table: Table,
                        partitionTS: LocalDateTime): Unit = {
    // TODO: Check that partition-size and batch-size are correctly set (when loading the config)
    val newPartition = new PartitionLog(
      table.getTargetArea,
      table.getTargetVertical,
      target.getPath,
      partitionTS,
      table.getTargetPartitionSizeDuration,
      table.getTargetVersion,
      finishLog,
      0
    )
    store.persist(newPartition)
  }

}
