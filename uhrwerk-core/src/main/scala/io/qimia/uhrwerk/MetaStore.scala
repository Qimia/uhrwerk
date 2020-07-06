package io.qimia.uhrwerk

import java.nio.file.Path
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.models.config.{Dependency, Global, Step}
import io.qimia.uhrwerk.models.store.TaskLog
import io.qimia.uhrwerk.utils.{ConfigReader, TimeTools}
import collection.JavaConverters._
import javax.persistence.{EntityManager, Persistence}

object MetaStore {
  def getLastTaskLog(store: EntityManager, stepName: String): Option[TaskLog] = {
    val results = store
      .createQuery(
        s"FROM TaskLog WHERE stepName = '${stepName}'" +
          "ORDER BY runTs DESC"
        , classOf[TaskLog])
      .setMaxResults(1)
      .getResultList
      .asScala
    if (results.isEmpty) {
      Option.empty
    } else {
      Option(results.head)
    }
  }
}


class MetaStore(globalConf: Path, stepConf: Path) {
  val globalConfig: Global = ConfigReader.readGlobalConfig(globalConf)
  val stepConfig: Step = ConfigReader.readStepConfig(stepConf)

  val stepBatchSize = TimeTools.convertDurationToObj(stepConfig.getBatchSize)

  val storeFactory = Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")



  // For each LocalDateTime check if all dependencies are there or tell which ones are not
  def checkDependencies(batchedDependencies: Seq[Dependency],
                        startTimes: Seq[LocalDateTime])
    : List[Either[List[String], LocalDateTime]] = {
    // Should be simple one on one check
    Nil
  }

  def writeStartTask(): TaskLog = {
    // Make sure that getting the latest runNr and incrementing it is done
    // in 1 transaction
    val store = storeFactory.createEntityManager
    store.getTransaction.begin()
    val previousRun = MetaStore.getLastTaskLog(store, stepConfig.getName)
    // TODO: For now version without storing the config (but should be configurable / devmode)
    val startLog: TaskLog = if (previousRun.isDefined) {
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
    store.getTransaction.commit()
    store.close()
    // TODO: Transaction failure??
    startLog
  }

  def writeFinishTask(startLog: TaskLog, partitionTS: LocalDateTime) = {

    // First write finish log based on success or failure

    // if success then also call writePartitionLog next

  }

  def writePartitionLog(store: EntityManager, finishLog: TaskLog, partitionTS: LocalDateTime) = {}

}
