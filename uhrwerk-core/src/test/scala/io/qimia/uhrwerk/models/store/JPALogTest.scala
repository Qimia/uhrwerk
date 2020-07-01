package io.qimia.uhrwerk.models.store

import java.time.{Duration, LocalDateTime}
import java.util.Date

import javax.persistence.{EntityManagerFactory, Persistence}
import org.scalatest.flatspec.AnyFlatSpec

import collection.JavaConverters._

class JPALogTest extends AnyFlatSpec {

  "When using JPA it" should "store and return PartitionLog entities" in {
    var entityManagerFactory: EntityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")

    val entityManager = entityManagerFactory.createEntityManager
    entityManager.getTransaction().begin()
    val testObj = new PartitionLog(
      "area_a",
      "vert_v",
      1,
      "/some/path/on/disk",
      LocalDateTime.now(),
      Duration.ofHours(1),
      1,
      123,
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
    for (res <- results) {
      System.out.println("PartitionLog (" + res.getArea + ") : " + res.getPartitionDuration.toString)
    }
    entityManager2.getTransaction.commit()
    entityManager2.close()

    entityManagerFactory.close()
  }

}
