package io.qimia.uhrwerk.metastore

import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.MetaStore
import io.qimia.uhrwerk.models.store.TaskLog
import javax.persistence.{EntityManagerFactory, Persistence}
import org.scalatest.flatspec.AnyFlatSpec

class MetaStoreUnitTest extends AnyFlatSpec {

  "getLastTaskLog helper function" should "get the latest tasklog" in {
    val stepName = "japjapjap"

    val entityManagerFactory = Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")
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

}
