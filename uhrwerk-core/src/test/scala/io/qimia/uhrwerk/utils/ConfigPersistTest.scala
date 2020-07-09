package io.qimia.uhrwerk.utils

import java.nio.file.Paths

import io.qimia.uhrwerk.models.config.{Connection, Global}
import io.qimia.uhrwerk.models.store._
import javax.persistence.{EntityManagerFactory, Persistence}
import org.scalatest.flatspec.AnyFlatSpec
import collection.JavaConverters._

class ConfigPersistTest extends AnyFlatSpec {
  "A step" should "be storable in db" in {
    val entityManagerFactory: EntityManagerFactory =
      Persistence.createEntityManagerFactory("io.qimia.uhrwerk.models")
    val entityManager = entityManagerFactory.createEntityManager

    // Note: Relies on the ConfigReader to function (not pure unit test)
    val stepPath = Paths.get("src/test/resources/config/step_test_1.yml")
    val stepConf = ConfigReader.readStepConfig(stepPath)
    val connection1 = new Connection()
    connection1.setName("connection_name")
    connection1.setType("jdbc")
    connection1.setJdbcUri("jdbc:mysql://host1:33060/sakila")
    val connection2 = new Connection()
    connection2.setName("connection_name2")
    connection2.setType("fs")
    connection2.setStartPath("/path/to/source")
    val connections = Array(connection1, connection2)
    val globalConf = new Global
    globalConf.setConnections(connections)

    var transaction = entityManager.getTransaction
    transaction.begin()
    ConfigPersist.persistStep(entityManager, stepConf, globalConf)
    transaction.commit()

    transaction = entityManager.getTransaction
    transaction.begin()
    val allConnections = entityManager.createQuery(
      "FROM ConnectionConfig",
      classOf[ConnectionConfig]
    ).getResultList().asScala
    val allDependencies = entityManager.createQuery(
      "FROM DependencyConfig",
      classOf[DependencyConfig]
    ).getResultList().asScala
    val allSteps = entityManager.createQuery(
      "FROM StepConfig",
      classOf[StepConfig]
    ).getResultList().asScala
    val allTables = entityManager.createQuery(
      "FROM TableInfo",
      classOf[TableInfo]
    ).getResultList().asScala
    val allTargets = entityManager.createQuery(
      "FROM TargetConfig",
      classOf[TargetConfig]
    ).getResultList().asScala
    transaction.commit()

    // It does not store connections which are not used
    assert(allConnections.length == 2)
    assert(allDependencies.length == 2)
    assert(allTargets.length == 1)
    assert(allTables.length == 3)
    assert(allSteps.length == 1)

    val tableConnectionNames = allTables.map(_.getConnectionName).toSet
    val connectionNames = allConnections.map(_.getName).toSet
    val checkConnectionNames = Set(
      "connection_name",
      "connection_name2"
    )
    assert(tableConnectionNames === checkConnectionNames)
    assert(connectionNames === checkConnectionNames)
    val tablePaths = allTables.map(_.getPath).toSet
    val checkTablePaths = Set(
      "schema.table",
      "someplace/other_table",
      "someplace/new_table"
    )
    assert(checkTablePaths === tablePaths)
  }

}
