package io.qimia.uhrwerk.models.store

import java.time.{Duration, LocalDateTime}
import java.util.Date

import javax.persistence.{EntityManagerFactory, Persistence}
import org.scalatest.flatspec.AnyFlatSpec

import collection.JavaConverters._

class JPALogTest extends AnyFlatSpec {

  "When using JPA it" should "store and return PartitionLog entities" in {
    val entityManagerFactory: EntityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")

    val entityManager = entityManagerFactory.createEntityManager
    entityManager.getTransaction().begin()
    val testObj = new PartitionLog(
      "area_a",
      "vert_v",
      "/some/path/on/disk",
      LocalDateTime.now(),
      Duration.ofHours(1),
      1,
      null,
      0
    )
    entityManager.persist(testObj)
    entityManager.getTransaction().commit()
    entityManager.close()

    val entityManager2 = entityManagerFactory.createEntityManager
    entityManager2.getTransaction.begin()
    val results = entityManager2
      .createQuery("from PartitionLog", classOf[PartitionLog])
      .getResultList
      .asScala
    entityManager2.getTransaction.commit()
    for (res <- results) {
      System.out.println("PartitionLog (" + res.getArea + ") : " + res.getPartitionDuration.toString)
    }
    entityManager2.close()

    entityManagerFactory.close()
  }

  it should "Store TaskLogs with or without config references too" in {
    val entityManagerFactory: EntityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")

    val entityManager = entityManagerFactory.createEntityManager
    entityManager.getTransaction().begin()
    val testObjWithoutRef = new TaskLog(
      "stepqimia",
      1,
      1,
      LocalDateTime.now(),
      Duration.ofHours(2),
      1
    )
    entityManager.persist(testObjWithoutRef)
    entityManager.getTransaction().commit()

    val entityManager2 = entityManagerFactory.createEntityManager
    entityManager2.getTransaction.begin()
    val results = entityManager2
      .createQuery("FROM TaskLog WHERE stepName = 'stepqimia'", classOf[TaskLog])
      .getResultList
      .asScala
    entityManager2.getTransaction.commit()
    assert(results.length == 1)
    assert(results.head.getRunDuration === Duration.ofHours(2))
    assert(results.head.getStep === null)

    val refStep = new StepConfig(
      "refstep",
      Duration.ofMinutes(15),
      4,
      20,
      "oneOnOne"
    )
    val testObjWithRef = new TaskLog(
      "refstep",
      refStep,
      1,
      10,
      LocalDateTime.now(),
      Duration.ofHours(2),
      1
    )
    entityManager.getTransaction().begin()
    entityManager.persist(refStep)
    entityManager.persist(testObjWithRef)
    entityManager.getTransaction().commit()
    entityManager.close()

    entityManager2.getTransaction.begin()
    val results2 = entityManager2
      .createQuery("FROM TaskLog WHERE stepName = 'refstep'", classOf[TaskLog])
      .getResultList
      .asScala
    entityManager2.getTransaction.commit()
    assert(results2.head.getVersion == 10)
    val stepOut = results2.head.getStep
    assert(stepOut.getMaxBatches == 20)

    entityManager2.close()
  }


}
