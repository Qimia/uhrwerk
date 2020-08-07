package io.qimia.uhrwerk.backend

import java.nio.file.{Path, Paths}
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.MetaStore
import io.qimia.uhrwerk.backend.jpa.{PartitionLog, TaskLog}
import io.qimia.uhrwerk.config.TaskLogType
import io.qimia.uhrwerk.config.model.Dependency
import javax.persistence.{EntityManagerFactory, Persistence}
import org.scalatest.flatspec.AnyFlatSpec

import scala.io.{BufferedSource, Source}

class MetaStoreUnitTest extends AnyFlatSpec {

  "getLastTaskLog helper function" should "get the latest tasklog" in {
    val tableName = "testhelperfunction"

    val entityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.backend.jpa")
    val entityManager = entityManagerFactory.createEntityManager()

    // At the start there should be nothing
    val entityRequestManager = entityManagerFactory.createEntityManager()
    entityRequestManager.getTransaction().begin()
    val res1 = MetaStore.getLastTaskLog(entityRequestManager, tableName)
    entityRequestManager.getTransaction().commit()
    assert(res1.isEmpty)

    entityManager.getTransaction().begin()
    val testObj1 = new TaskLog(
      tableName,
      1,
      1,
      LocalDateTime.of(2000, 1, 1, 0, 0, 0),
      Duration.ZERO,
      TaskLogType.SUCCESS
    )
    entityManager.persist(testObj1)
    val testObj2 = new TaskLog(
      tableName,
      1,
      1,
      LocalDateTime.of(2000, 1, 2, 0, 0, 0),
      Duration.ZERO,
      TaskLogType.SUCCESS
    )
    entityManager.persist(testObj2)
    val lastDate = LocalDateTime.of(2000, 1, 3, 0, 0, 0)
    val testObj3 = new TaskLog(
      tableName,
      1,
      1,
      lastDate,
      Duration.ZERO,
      TaskLogType.SUCCESS
    )
    entityManager.persist(testObj3)
    entityManager.getTransaction().commit()
    entityManager.close()

    entityRequestManager.getTransaction().begin()
    val res2 = MetaStore.getLastTaskLog(entityRequestManager, tableName)
    entityRequestManager.getTransaction().commit()
    entityRequestManager.close()
    assert(res2.get.getTableName === tableName)
    assert(res2.get.getRunTs === lastDate)
  }

  "getDependencies helper function" should "only get the right dependencies" in {
    val entityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.backend.jpa")
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
        "table_1",
        LocalDateTime.of(2000, 1, 1, 2, 0, 0),
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
        LocalDateTime.of(2000, 1, 1, 3, 0, 0),
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
    dependencies(0).setPartitionSize("1h")
    dependencies(1).setPartitionSize("1h")
    val res1 = MetaStore.getBatchedDependencies(
      entityManager,
      dependencies,
      List(LocalDateTime.of(2000, 1, 1, 0, 0, 0)),
      Duration.ofHours(1))
    entityManager.getTransaction.commit()
    assert(res1.head === Right(LocalDateTime.of(2000, 1, 1, 0, 0, 0)))

    entityManager.getTransaction().begin()
    val res2 = MetaStore.getBatchedDependencies(
      entityManager,
      dependencies,
      List(LocalDateTime.of(2000, 1, 1, 1, 0, 0)),
      Duration.ofHours(1))
    entityManager.getTransaction().commit()
    res2.head match {
      case Right(_) => fail
      case Left(y)  => assert(y._2.head === "table_2")
    }

    entityManager.getTransaction().begin()
    val res3 = MetaStore.getBatchedDependencies(
      entityManager,
      dependencies,
      List(LocalDateTime.of(2020, 1, 1, 1, 0, 0)),
      Duration.ofHours(1))
    entityManager.getTransaction().commit()
    res3.head match {
      case Right(_) => fail
      case Left(y)  => assert(y._2.size == 2)
    }

    entityManager.getTransaction().begin()
    val queryTimes = List(LocalDateTime.of(2000, 1, 1, 1, 0, 0),
                          LocalDateTime.of(2000, 1, 1, 3, 0, 0))
    val res4 = MetaStore.getBatchedDependencies(entityManager,
                                                Array(dependencies(0)),
                                                queryTimes,
                                                Duration.ofHours(1))
    entityManager.getTransaction().commit()
    assert(res4 === queryTimes.map(x => Right(x)))
  }

  "MetaStore object" should "read TaskLogs and write TaskLogs" in {
    val globalConfig: Path =
      Paths.get(getClass.getResource("/config/global_test_1.yml").getPath)
    val stepConfig: Path =
      Paths.get(getClass.getResource("/config/table_test_1.yml").getPath)

    val metaStore = MetaStore(globalConfig, stepConfig, false)
    List("mysql_test", "s3_test", "local_filesystem_test").foreach(name =>
      assert(metaStore.connections.contains(name)))
    val startTask = metaStore.logStartTask()
    val partitionDate = LocalDateTime.of(2000, 1, 1, 0, 0, 0)

    val entityManager = metaStore.storeFactory.createEntityManager()
    entityManager.getTransaction.begin()
    var latestTaskLog = MetaStore.getLastTaskLog(entityManager, "load_a_table")
    entityManager.getTransaction.commit()
    assert(latestTaskLog.get.getLogType === TaskLogType.START) // Task Started

    metaStore.logFinishTask(startTask, partitionDate, success = false)
    entityManager.getTransaction.begin()
    latestTaskLog = MetaStore.getLastTaskLog(entityManager, "load_a_table")
    entityManager.getTransaction.commit()
    assert(latestTaskLog.get.getLogType === TaskLogType.FAILURE) // Task failed

    val partitionDate2 = LocalDateTime.of(2001, 1, 1, 0, 0, 0)
    entityManager.getTransaction.begin()
    val startTask2 = metaStore.logStartTask()
    metaStore.logFinishTask(startTask2, partitionDate2, success = true)
    entityManager.getTransaction.commit()
    entityManager.getTransaction.begin()
    latestTaskLog = MetaStore.getLastTaskLog(entityManager, "load_a_table")
    entityManager.getTransaction.commit()
    assert(latestTaskLog.get.getLogType === TaskLogType.SUCCESS) // Task succeeded
  }

}
