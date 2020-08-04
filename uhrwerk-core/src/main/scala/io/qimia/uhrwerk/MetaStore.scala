package io.qimia.uhrwerk

import java.nio.file.Path
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.MetaStore.{DependencyFailed, DependencySuccess}
import io.qimia.uhrwerk.models.TaskLogType
import io.qimia.uhrwerk.models.config.{Dependency, Global, Step, Target}
import io.qimia.uhrwerk.models.store.{PartitionLog, TaskLog}
import io.qimia.uhrwerk.utils.{ConfigPersist, ConfigReader, TimeTools}

import collection.JavaConverters._
import javax.persistence.{EntityManager, Persistence}

import scala.collection.mutable

object MetaStore {
  type DependencyFailed = (LocalDateTime, Set[String])
  type DependencySuccess = LocalDateTime

  // Get latest Tasklog (could be any type: success failure or start)
  def getLastTaskLog(store: EntityManager,
                     stepName: String): Option[TaskLog] = {
    val results = store
      .createQuery(s"FROM TaskLog WHERE stepName = '${stepName}'" +
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
                             startTimes: Seq[LocalDateTime])
    : List[Either[DependencyFailed, DependencySuccess]] = {
    // TODO: Create groupBy version when figured out howto filter each dependency with its version number
    // (Could be done with a very big WHERE (a-specs OR b-specs OR c-specs) clause, not sure if there is a performance gain)

    val startTime = startTimes.head
    val endTime = startTimes.last
    val storedPartitions: mutable.Map[LocalDateTime, Set[String]] =
      new mutable.HashMap().withDefaultValue(Set())

    def updateStoredPartitions(dep: Dependency): Unit = {
      val depName = dep.getPath  // TODO: If Dependencies gets a proper name / key field use that instead
      val results = store
        .createQuery(
          s"FROM PartitionLog WHERE area = '${dep.getArea}' " +
            s"AND vertical = '${dep.getVertical}' " +
            s"AND path = '${depName}' " +
            s"AND version = ${dep.getVersion} " +
            s"AND partitionDuration = :partDur " +
            s"AND partitionTs BETWEEN :partstart AND :partfinish",
          // TODO: Need to check connection
          classOf[PartitionLog]
        )
        .setParameter("partDur",dep.getPartitionSizeDuration)
        .setParameter("partstart", startTime)
        .setParameter("partfinish", endTime)
        .getResultList
        .asScala
      results
        .map(_.getPartitionTs)
        .foreach(ldt => storedPartitions(ldt) += depName)
    }
    batchedDependencies.foreach(updateStoredPartitions)

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

  def apply(globalConf: Path, stepConf: Path): MetaStore = {
    val globalConfig: Global = ConfigReader.readGlobalConfig(globalConf)
    val stepConfig: Step = ConfigReader.readStepConfig(stepConf)
    new MetaStore(globalConfig, stepConfig)
  }
}

class MetaStore(globalConf: Global,
                stepConf: Step,
                persist: Boolean = false) {
  // TODO: After loading the config we should check if it is correct (if it's in a valid state)
  val globalConfig = globalConf
  val stepConfig = stepConf
  val connections = globalConfig.getConnections.map(conn => conn.getName -> conn).toMap
  val storeFactory =
    Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")

  // TODO: Optional persistence of the configurations
  val persistedConf: Option[ConfigPersist.PersistStruc] = if (persist) {
    val store = storeFactory.createEntityManager
    store.getTransaction.begin()
    val res = ConfigPersist.persistStep(store, stepConfig, globalConfig)
    store.getTransaction.commit()
    Option(res)
  } else {
    Option.empty
  }

  // For each LocalDateTime check if all dependencies are there or tell which ones are not
  // !! Assumes the startTimes are ordered and continuous (no gaps) !!
  def checkDependencies(batchedDependencies: Array[Dependency],
                        startTimes: Seq[LocalDateTime])
    : List[Either[DependencyFailed, DependencySuccess]] = {
    // Should be simple one on one check
    val store = storeFactory.createEntityManager
    val ta = store.getTransaction
    ta.begin()
    val res =
      MetaStore.getBatchedDependencies(store, batchedDependencies, startTimes)
    ta.commit()
    store.close()
    res
  }

  // Write a TaskLog that a task has been started and return the log object for writing the matching finish object
  def logStartTask(): TaskLog = {
    // Make sure that getting the latest runNr and incrementing it is done
    // in 1 transaction
    val store = storeFactory.createEntityManager

    val ta = store.getTransaction
    ta.begin
    val previousRun = MetaStore.getLastTaskLog(store, stepConfig.getName)
    // TODO: For now version without storing the config (but should be configurable / devmode)
    val previousRunNr = if (previousRun.isDefined) {
      previousRun.get.getRunNumber + 1
    } else {
      1
    }
    val startLog = if (persistedConf.isDefined) {
      new TaskLog(
        stepConfig.getName,
        persistedConf.get._1,
        previousRunNr, // How often did this step run
        stepConfig.getVersion,
        LocalDateTime.now(),
        Duration.ZERO,
        TaskLogType.START
      )
    } else {
      new TaskLog(
        stepConfig.getName,
        previousRunNr, // How often did this step run
        stepConfig.getVersion,
        LocalDateTime.now(),
        Duration.ZERO,
        TaskLogType.START
      )
    }
    store.persist(startLog)
    ta.commit()
    store.close()
    // TODO: Transaction failure??
    startLog
  }

  // write that a Task is finished, could be successful or not (this also in turn will write the target's partition-logs
  // if it were successful
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
    val finishLog = if (persistedConf.isDefined) {
      new TaskLog(
        stepConfig.getName,
        persistedConf.get._1,
        startLog.getRunNumber(), // How often did this step run
        stepConfig.getVersion,
        timeNow,
        Duration.between(startLog.getRunTs, timeNow),
        logType
      )
    } else {
      new TaskLog(
        stepConfig.getName,
        startLog.getRunNumber(),
        stepConfig.getVersion,
        timeNow,
        Duration.between(startLog.getRunTs, timeNow),
        logType
      )
    }
    store.persist(finishLog)

    // if success then also call writePartitionLog next
    // Either fails completely or writes all tasklogs (no partial completion)
    if (success) {
      stepConfig.getTargets.foreach(t =>
          this.writePartitionLog(store, finishLog, t, partitionTS)
      )
    }
    ta.commit()
    store.close()
  }

  // Write that a partition has been written for a single Target and a single Timestamp
  def writePartitionLog(store: EntityManager,
                        finishLog: TaskLog,
                        target: Target,
                        partitionTS: LocalDateTime) = {
    // TODO: Check that partition-size and batch-size are correctly set (when loading the config)
    val tablePath = target.getPath
    val connectionConf = persistedConf.flatMap(_._2.get(tablePath))
    val newPartition = if (connectionConf.isDefined) {
      // TODO: Can easily fail if connection hasn't been stored properly
      new PartitionLog(
        target.getArea,
        target.getVertical,
        connectionConf.get, // Insert reference to already stored connection config
        tablePath,
        partitionTS,
        target.getPartitionSizeDuration,
        target.getVersion,
        finishLog,
        0
      )
    } else {
      new PartitionLog(
        target.getArea,
        target.getVertical,
        target.getPath,
        partitionTS,
        target.getPartitionSizeDuration,
        target.getVersion,
        finishLog,
        0
      )
    }
    store.persist(newPartition)
  }

}
