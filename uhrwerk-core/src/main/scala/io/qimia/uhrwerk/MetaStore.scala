package io.qimia.uhrwerk

import java.nio.file.Path
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.MetaStore.{DependencyFailed, DependencySuccess}
import io.qimia.uhrwerk.models.config.{Dependency, Global, Step, Target}
import io.qimia.uhrwerk.models.store.{PartitionLog, TaskLog}
import io.qimia.uhrwerk.utils.{ConfigReader, TimeTools}

import collection.JavaConverters._
import javax.persistence.{EntityManager, Persistence}

import scala.collection.mutable

object MetaStore {
  type DependencyFailed = (LocalDateTime, Set[String])
  type DependencySuccess = LocalDateTime

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

    val startTime = startTimes.head
    val endTime = startTimes.last
    val storedPartitions: mutable.Map[LocalDateTime, Set[String]] =
      new mutable.HashMap().withDefaultValue(Set())
    batchedDependencies.foreach(
      dep => {
        val depName = dep.getPath
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
          .setParameter("partDur",
                        TimeTools.convertDurationToObj(dep.getPartitionSize))
          .setParameter("partstart", startTime)
          .setParameter("partfinish", endTime)
          .getResultList
          .asScala
        results
          .map(_.getPartitionTs)
          .foreach(ldt => storedPartitions(ldt) += depName)
        // Want to track exactly which dependencies are not met
      }
    )

    val requiredDependencies = batchedDependencies.map(_.getPath).toSet
    startTimes.map(startTime => {
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
    }).toList
  }
}

class MetaStore(globalConf: Path, stepConf: Path) {
  val globalConfig: Global = ConfigReader.readGlobalConfig(globalConf)
  val stepConfig: Step = ConfigReader.readStepConfig(stepConf)

  val stepBatchSize = TimeTools.convertDurationToObj(stepConfig.getBatchSize)
  val storeFactory =
    Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")

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

  def writeStartTask(): TaskLog = {
    // Make sure that getting the latest runNr and incrementing it is done
    // in 1 transaction
    val store = storeFactory.createEntityManager

    val ta = store.getTransaction
    ta.begin
    val previousRun = MetaStore.getLastTaskLog(store, stepConfig.getName)
    // TODO: For now version without storing the config (but should be configurable / devmode)
    val startLog = if (previousRun.isDefined) {
      new TaskLog(
        stepConfig.getName,
        previousRun.get.getRunNumber + 1, // How often did this step run
        stepConfig.getVersion,
        LocalDateTime.now(),
        Duration.ZERO,
        0 // meaning task started running
      )
    } else {
      new TaskLog(
        stepConfig.getName,
        1,
        stepConfig.getVersion,
        LocalDateTime.now(),
        Duration.ZERO,
        0 // meaning task started running
      )
    }
    store.persist(startLog)
    ta.commit()
    store.close()
    // TODO: Transaction failure??
    startLog
  }

  def writeFinishTask(startLog: TaskLog,
                      partitionTS: LocalDateTime,
                      success: Boolean) = {
    val store = storeFactory.createEntityManager

    // First write finish log based on success or failure
    val ta = store.getTransaction
    ta.begin
    val timeNow = LocalDateTime.now()
    val finishLog = if (success) {
      new TaskLog(
        stepConfig.getName,
        startLog.getRunNumber(), // How often did this step run
        stepConfig.getVersion,
        timeNow,
        Duration.between(startLog.getRunTs, timeNow),
        1 // meaning task started running
      )
    } else {
      new TaskLog(
        stepConfig.getName,
        startLog.getRunNumber(),
        stepConfig.getVersion,
        timeNow,
        Duration.between(startLog.getRunTs, timeNow),
        2 // meaning task started running
      )
    }
    store.persist(finishLog)

    // if success then also call writePartitionLog next
    if (success) {
      stepConfig.getTargets.foreach(t =>
        this.writePartitionLog(store, finishLog, t, partitionTS))
    }
    ta.commit()
    store.close()
  }

  def writePartitionLog(store: EntityManager,
                        finishLog: TaskLog,
                        target: Target,
                        partitionTS: LocalDateTime) = {
    // TODO: Check that partition-size and batch-size are correctly set (when loading the config)
    val newPartition = new PartitionLog(
      target.getArea,
      target.getVertical,
      target.getPath,
      partitionTS,
      TimeTools.convertDurationToObj(target.getPartitionSize),
      target.getVersion,
      finishLog,
      0
    )
    store.persist(newPartition)
  }

}
