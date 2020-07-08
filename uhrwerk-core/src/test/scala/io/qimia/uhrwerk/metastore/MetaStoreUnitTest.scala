package io.qimia.uhrwerk.metastore

import java.nio.file.Paths
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.MetaStore
import io.qimia.uhrwerk.models.store.{PartitionLog, TaskLog}
import io.qimia.uhrwerk.models.config.Dependency
import javax.persistence.{EntityManagerFactory, Persistence}
import org.scalatest.flatspec.AnyFlatSpec

class MetaStoreUnitTest extends AnyFlatSpec {

  "getLastTaskLog helper function" should "get the latest tasklog" in {
    val stepName = "testhelperfunction"

    val entityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")
    val entityManager = entityManagerFactory.createEntityManager()

    // At the start there should be nothing
    val entityRequestManager = entityManagerFactory.createEntityManager()
    entityRequestManager.getTransaction().begin()
    val res1 = MetaStore.getLastTaskLog(entityRequestManager, stepName)
    entityRequestManager.getTransaction().commit()
    assert(res1.isEmpty)

    entityManager.getTransaction().begin()
    val testObj1 = new TaskLog(
      stepName,
      1,
      1,
      LocalDateTime.of(2000, 1, 1, 0, 0, 0),
      Duration.ZERO,
      1
    )
    entityManager.persist(testObj1)
    val testObj2 = new TaskLog(
      stepName,
      1,
      1,
      LocalDateTime.of(2000, 1, 2, 0, 0, 0),
      Duration.ZERO,
      1
    )
    entityManager.persist(testObj2)
    val lastDate = LocalDateTime.of(2000, 1, 3, 0, 0, 0)
    val testObj3 = new TaskLog(
      stepName,
      1,
      1,
      lastDate,
      Duration.ZERO,
      1
    )
    entityManager.persist(testObj3)
    entityManager.getTransaction().commit()
    entityManager.close()

    entityRequestManager.getTransaction().begin()
    val res2 = MetaStore.getLastTaskLog(entityRequestManager, stepName)
    entityRequestManager.getTransaction().commit()
    entityRequestManager.close()
    assert(res2.get.getStepName === stepName)
    assert(res2.get.getRunTs === lastDate)
  }

  "getDependencies helper function" should "only get the right dependencies" in {
    val entityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")
    val entityManager = entityManagerFactory.createEntityManager()
    entityManager.getTransaction().begin()
    entityManager.persist(
      new PartitionLog(
        "area_a",
        "vertical_v",
        "table_1",
        LocalDateTime.of(2000, 1, 1, 0, 0, 0),
        Duration.ofHours(1),
        1,
        null,
        0
      ))
    entityManager.persist(
      new PartitionLog(
        "area_a",
        "vertical_v",
        "table_1",
        LocalDateTime.of(2000, 1, 1, 1, 0, 0),
        Duration.ofHours(1),
        1,
        null,
        0
      ))
    entityManager.persist(
      new PartitionLog(
        "area_a",
        "vertical_v",
        "table_2",
        LocalDateTime.of(2000, 1, 1, 0, 0, 0),
        Duration.ofHours(1),
        1,
        null,
        0
      ))
    entityManager.getTransaction.commit()

    entityManager.getTransaction().begin()
    val dependencies = Array(
      new Dependency,
      new Dependency
    )
    dependencies(0).setArea("area_a")
    dependencies(1).setArea("area_a")
    dependencies(0).setVertical("vertical_v")
    dependencies(1).setVertical("vertical_v")
    dependencies(0).setVersion(1)
    dependencies(1).setVersion(1)
    dependencies(0).setPath("table_1")
    dependencies(1).setPath("table_2")
    val res1 = MetaStore.getBatchedDependencies(
      entityManager,
      dependencies,
      List(LocalDateTime.of(2000, 1, 1, 0, 0, 0)))
    entityManager.getTransaction.commit()
    assert(res1.head === Right(LocalDateTime.of(2000, 1, 1, 0, 0, 0)))

    entityManager.getTransaction().begin()
    val res2 = MetaStore.getBatchedDependencies(
      entityManager,
      dependencies,
      List(LocalDateTime.of(2000, 1, 1, 1, 0, 0)))
    entityManager.getTransaction().commit()
    res2.head match {
      case Right(_) => fail
      case Left(y) => assert(y._2.head === "table_2")
    }

    entityManager.getTransaction().begin()
    val res3 = MetaStore.getBatchedDependencies(
      entityManager,
      dependencies,
      List(LocalDateTime.of(2020, 1, 1, 1, 0, 0)))
    entityManager.getTransaction().commit()
    res3.head match {
      case Right(_) => fail
      case Left(y) => assert(y._2.size == 2)
    }
  }

  "MetaStore object" should "read TaskLogs and write TaskLogs" in {
    val globalConfig = Paths.get("src/test/resources/config/global_test_1.yml")
    val stepConfig = Paths.get("src/test/resources/config/step_test_1.yml")

    val metaStore = new MetaStore(globalConfig, stepConfig)
    val startTask = metaStore.writeStartTask()
    val partitionDate = LocalDateTime.of(2000, 1, 1, 0, 0, 0)

    val entityManager = metaStore.storeFactory.createEntityManager()
    entityManager.getTransaction.begin()
    var latestTaskLog = MetaStore.getLastTaskLog(entityManager, "load_a_table")
    entityManager.getTransaction.commit()
    assert(latestTaskLog.get.getLogType == 0) // Task Started

    metaStore.writeFinishTask(startTask, partitionDate, success = false)
    entityManager.getTransaction.begin()
    latestTaskLog = MetaStore.getLastTaskLog(entityManager, "load_a_table")
    entityManager.getTransaction.commit()
    assert(latestTaskLog.get.getLogType == 2) // Task failed

    val partitionDate2 = LocalDateTime.of(2001, 1, 1, 0, 0, 0)
    entityManager.getTransaction.begin()
    val startTask2 = metaStore.writeStartTask()
    metaStore.writeFinishTask(startTask2, partitionDate2, success = true)
    entityManager.getTransaction.commit()
    entityManager.getTransaction.begin()
    latestTaskLog = MetaStore.getLastTaskLog(entityManager, "load_a_table")
    entityManager.getTransaction.commit()
    assert(latestTaskLog.get.getLogType == 1)  // Task succeeded
  }

}
