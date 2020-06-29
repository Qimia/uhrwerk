package io.qimia.uhrwerk.models

import java.util
import java.util.{Date, List}

import javax.persistence.{EntityManagerFactory, Persistence}
import org.scalatest.flatspec.AnyFlatSpec
import collection.JavaConverters._

class EntityManagerIllustrationTest extends AnyFlatSpec {

  "When using JPA it" should "store and return Event entities" in {
    var entityManagerFactory: EntityManagerFactory = Persistence.createEntityManagerFactory("org.hibernate.tutorial.jpa")

    val entityManager = entityManagerFactory.createEntityManager
    entityManager.getTransaction().begin()
    entityManager.persist(new Event(Option("Our very first event!"), new Date()))
    entityManager.persist(new Event(Option("A follow up event"), new Date()))
    entityManager.persist(new Event(Option.empty, new Date()))
    entityManager.getTransaction().commit()
    entityManager.close()


    val entityManager2 = entityManagerFactory.createEntityManager
    entityManager2.getTransaction.begin()
    val result = entityManager2.createQuery("from Event", classOf[Event]).getResultList.asScala
    for (event <- result) {
      System.out.println("Event (" + event.date + ") : " + event.title)
    }
    entityManager2.getTransaction.commit()
    entityManager2.close()

    entityManagerFactory.close()
  }

}
