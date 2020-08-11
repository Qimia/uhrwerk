package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.MetaStore
import io.qimia.uhrwerk.config.model.{Connection, Dependency, Global, Source, Table, Target}
import org.scalatest.flatspec.AnyFlatSpec

class ConfigProcessTest extends AnyFlatSpec {

  "given a single batchsize all unknown partitioned tables" should "use the step's batchsize" in {
    val someTable = new Table()
    someTable.setName("some_step")
    someTable.setBatchSize("30m")
    val sourceA = new Source
    val sources = Array(sourceA)
    someTable.setSources(sources)
    val dependencyA = new Dependency
    val dependencyB = new Dependency
    dependencyB.setPartitionSize("15m")
    val dependencies = Array(dependencyA, dependencyB)
    someTable.setDependencies(dependencies)
    val targetA = new Target
    val targets = Array(targetA)
    someTable.setTargets(targets)
    ConfigProcess.autofillStepPartitionSizes(someTable)

    assert(someTable.getBatchSize == "30m")
    assert(sourceA.getPartitionSize == "30m")
    assert(dependencyA.getPartitionSize == "30m")
    assert(dependencyB.getPartitionSize == "15m")
    assert(someTable.getPartitionSize == "30m")
  }

  "given a global & step config with missing connection-name it" should "not validate the combination" in {
    val someStep = new Table()
    someStep.setName("some_step")
    val sourceA = new Source
    sourceA.setFormat("parquet")
    sourceA.setPath("a/b/c")
    sourceA.setConnectionName("unknown_connection")
    val sources = Array(sourceA)
    someStep.setSources(sources)

    val someGlobal = new Global()
    val wrongConnection = new Connection()
    wrongConnection.setName("an_unused_connection")
    someGlobal.setConnections(Array(wrongConnection))
    val res = ConfigProcess.checkFieldsConfig(someStep, someGlobal)
    assert(!res)
  }

  "given a proper step and global configuration it" should "be filled in and validated correctly" in {
    val goodGlobal = new Global()
    val connectionA = new Connection()
    connectionA.setName("conn1")
    connectionA.setConnectionUrl("/path/on/shared/disk/")
    val connectionB = new Connection()
    connectionB.setName("conn2")
    connectionB.setCloudId("abc123")
    connectionB.setCloudPass("secret_pass")
    connectionB.setCloudRegion("us-west-10")
    connectionB.setConnectionUrl("s3://uhrwerk-datalake-bucket/key/to/lake/")
    val connectionC = new Connection()
    connectionC.setName("conn3")
    connectionC.setJdbcDriver("org.mariadb.jdbc.Driver")
    connectionC.setConnectionUrl("jdbc:mariadb://localhost:3306/db")
    connectionC.setUser("root")
    connectionC.setPass("myPassword")
    val backendConnection = new Connection()
    backendConnection.setName(ConfigProcess.UHRWERK_BACKEND_CONNECTION_NAME)
    backendConnection.setJdbcDriver("org.mariadb.jdbc.Driver")
    backendConnection.setConnectionUrl("jdbc:mariadb://localhost:3306/db")
    backendConnection.setUser("root")
    backendConnection.setPass("myPassword")
    goodGlobal.setConnections(Array(connectionA, connectionB, connectionC, backendConnection))


    val goodTable = new Table()
    goodTable.setBatchSize("30m")
    val dependencyA = new Dependency()
    dependencyA.setConnectionName("conn1")
    dependencyA.setFormat("parquet")
    dependencyA.setTableName("tableOne")
    val dependencyB = new Dependency()
    dependencyB.setConnectionName("conn2")
    dependencyB.setType("window")
    dependencyB.setPartitionSize("30m")
    dependencyB.setTableName("tableTwo")
    dependencyB.setFormat("parquet")
    val dependencyC = new Dependency()
    dependencyC.setConnectionName("conn3")
    dependencyC.setType("agg")
    dependencyC.setPartitionCount(2)
    dependencyC.setFormat("jdbc")
    dependencyC.setPartitionColumn("created_at")
    goodTable.setDependencies(Array(dependencyA, dependencyB, dependencyC))
    val sourceA = new Source()
    sourceA.setConnectionName("conn3")
    sourceA.setFormat("externalschema.externaltable")
    sourceA.setPartitionColumn("created_at")
    goodTable.setSources(Array(sourceA))
    val targetA = new Target()
    targetA.setConnectionName("conn1")
    targetA.setFormat("core/tableTwo")
    goodTable.setTargets(Array(targetA))

    val res = ConfigProcess.enrichAndValidateConfig(goodTable, goodGlobal)
    assert(res)
    assert(dependencyA.getPartitionSize === "30m")
    assert(sourceA.getPartitionSize === "30m")
    assert(goodTable.getPartitionSize === "30m")
    assert(dependencyC.getPartitionSize === "15m")
  }

  "given some aggregate dependency with the wrong size it" should "not validate" in {
    val someStep1 = new Table()
    val aggDep = new Dependency()
    aggDep.setType("agg")
    aggDep.setPartitionSize("20m")
    someStep1.setDependencies(Array(aggDep))
    val target1 = new Target()
    someStep1.setPartitionSize("30m")
    someStep1.setTargets(Array(target1))
    val res1 = ConfigProcess.checkAndUpdateAgg(someStep1)
    assert(!res1)

    val someStep2 = new Table()
    val aggCountDep = new Dependency()
    aggCountDep.setType("aggregate")
    aggCountDep.setPartitionCount(7)
    someStep2.setDependencies(Array(aggCountDep))
    val target2 = new Target()
    someStep2.setPartitionSize("1h")
    someStep2.setTargets(Array(target2))

    val res2 = ConfigProcess.checkAndUpdateAgg(someStep2)
    assert(!res2)

    val someTable3 = new Table()
    val actuallyOneOnOne = new Dependency()
    actuallyOneOnOne.setType("agg")
    actuallyOneOnOne.setPartitionSize("30m")
    someTable3.setDependencies(Array(actuallyOneOnOne))
    someTable3.setTargets(Array(target1))
    someTable3.setPartitionSize("30m")
    val res3 = ConfigProcess.checkAndUpdateAgg(someTable3)
    assert(!res3)

    val someTable4 = new Table()
    val aggDoubleSetDep = new Dependency()
    aggDoubleSetDep.setType("agg")
    aggDoubleSetDep.setPartitionSize("15m")
    aggDoubleSetDep.setPartitionCount(3)
    someTable4.setDependencies(Array(aggDoubleSetDep))
    someTable4.setTargets(Array(target1))
    someTable4.setPartitionSize("30m")
    val res4 = ConfigProcess.checkAndUpdateAgg(someTable4)
    assert(!res4)
  }

  "given correct aggregate dependency, they" should "validate" in {
    val someTable1 = new Table()
    val aggDep = new Dependency()
    aggDep.setType("agg")
    aggDep.setPartitionSize("10m")
    someTable1.setDependencies(Array(aggDep))
    val target1 = new Target()
    someTable1.setPartitionSize("30m")
    someTable1.setTargets(Array(target1))
    val res1 = ConfigProcess.checkAndUpdateAgg(someTable1)
    assert(res1)

    val someTable2 = new Table()
    val aggCountDep = new Dependency()
    aggCountDep.setType("aggregate")
    aggCountDep.setPartitionCount(5)
    someTable2.setDependencies(Array(aggCountDep))
    val target2 = new Target()
    someTable2.setPartitionSize("1h")
    someTable2.setTargets(Array(target2))

    val res2 = ConfigProcess.checkAndUpdateAgg(someTable2)
    assert(res2)

    val someTable3 = new Table()
    aggCountDep.setPartitionSize("")
    aggCountDep.setPartitionCount(3)
    someTable3.setDependencies(Array(aggCountDep))
    someTable3.setPartitionSize("1h")
    someTable3.setTargets(Array(target2))

    val res3 = ConfigProcess.checkAndUpdateAgg(someTable3)
    assert(res3)

    val someTable4 = new Table()
    aggCountDep.setPartitionCount(1)
    aggCountDep.setPartitionSize("15m")
    someTable4.setDependencies(Array(aggCountDep))
    someTable4.setTargets(Array(target2))
    someTable4.setPartitionSize("1h")

    val res4 = ConfigProcess.checkAndUpdateAgg(someTable4)
    assert(res4)
  }
}
