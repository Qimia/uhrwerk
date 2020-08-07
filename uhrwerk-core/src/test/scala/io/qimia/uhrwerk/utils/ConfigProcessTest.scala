package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.config.model.{Connection, Dependency, Global, Source, Table, Target}
import org.scalatest.flatspec.AnyFlatSpec

class ConfigProcessTest extends AnyFlatSpec {

  "given a single batchsize all unknown partitioned tables" should "use the step's batchsize" in {
    val someStep = new Table()
    someStep.setName("some_step")
    someStep.setBatchSize("30m")
    val sourceA = new Source
    val sources = Array(sourceA)
    someStep.setSources(sources)
    val dependencyA = new Dependency
    val dependencyB = new Dependency
    dependencyB.setPartitionSize("15m")
    val dependencies = Array(dependencyA, dependencyB)
    someStep.setDependencies(dependencies)
    val targetA = new Target
    val targets = Array(targetA)
    someStep.setTargets(targets)
    ConfigProcess.autofillStepPartitionSizes(someStep)

    assert(someStep.getBatchSize == "30m")
    assert(sourceA.getPartitionSize == "30m")
    assert(dependencyA.getPartitionSize == "30m")
    assert(dependencyB.getPartitionSize == "15m")
    assert(targetA.getPartitionSize == "30m")
  }

  "given multiple target with different partition sizes to process" should "fail to validate" in {
    val targetA = new Target
    targetA.setPartitionSize("30m")
    val targetB = new Target
    targetB.setPartitionSize("15m")
    val aStep = new Table
    aStep.setTargets(Array(targetA, targetB))
    val res = ConfigProcess.checkAllTargetTimes(aStep)
    assert (!res)
  }

  "given multiple target with the same partition sizes " should "validate" in {
    val targetA = new Target
    targetA.setPartitionSize("15m")
    val targetB = new Target
    targetB.setPartitionSize("15m")
    val targetC = new Target
    targetC.setPartitionSize("15M")
    val aStep = new Table
    aStep.setTargets(Array(targetA, targetB, targetC))
    val res = ConfigProcess.checkAllTargetTimes(aStep)
    assert(res)
  }

  "given a global & step config with missing connection-name it" should "not validate the combination" in {
    val someStep = new Table()
    someStep.setName("some_step")
    val sourceA = new Source
    sourceA.setPath("a/b/c.parquet")
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
    connectionA.setStartPath("/path/on/shared/disk/")
    val connectionB = new Connection()
    connectionB.setName("conn2")
    connectionB.setCloudId("abc123")
    connectionB.setCloudPass("secret_pass")
    connectionB.setCloudRegion("us-west-10")
    val connectionC = new Connection()
    connectionC.setName("conn3")
    connectionC.setJdbcDriver("org.mariadb.jdbc.Driver")
    connectionC.setJdbcUrl("jdbc:mariadb://localhost:3306/db")
    connectionC.setUser("root")
    connectionC.setPass("myPassword")
    goodGlobal.setConnections(Array(connectionA, connectionB, connectionC))

    val goodStep = new Table()
    goodStep.setBatchSize("30m")
    val dependencyA = new Dependency()
    dependencyA.setConnectionName("conn1")
    dependencyA.setPath("core/tableOne")
    val dependencyB = new Dependency()
    dependencyB.setConnectionName("conn2")
    dependencyB.setType("window")
    dependencyB.setPartitionSize("30m")
    dependencyB.setPath("s3://uhrwerk-datalake-bucket/key/to/lake/table")
    val dependencyC = new Dependency()
    dependencyC.setConnectionName("conn3")
    dependencyC.setType("agg")
    dependencyC.setPartitionCount(2)
    dependencyC.setPath("schema.table")
    dependencyC.setPartitionColumn("created_at")
    goodStep.setDependencies(Array(dependencyA, dependencyB, dependencyC))
    val sourceA = new Source()
    sourceA.setConnectionName("conn3")
    sourceA.setPath("externalschema.externaltable")
    sourceA.setPartitionColumn("created_at")
    goodStep.setSources(Array(sourceA))
    val targetA = new Target()
    targetA.setConnectionName("conn1")
    targetA.setPath("core/tableTwo")
    goodStep.setTargets(Array(targetA))

    val res = ConfigProcess.enrichAndValidateConfig(goodStep, goodGlobal)
    assert(res)
    assert(dependencyA.getPartitionSize === "30m")
    assert(sourceA.getPartitionSize === "30m")
    assert(targetA.getPartitionSize === "30m")
    assert(dependencyC.getPartitionSize === "15m")
  }

  "given some aggregate dependency with the wrong size it" should "not validate" in {
    val someStep1 = new Table()
    val aggDep = new Dependency()
    aggDep.setType("agg")
    aggDep.setPartitionSize("20m")
    someStep1.setDependencies(Array(aggDep))
    val target1 = new Target()
    target1.setPartitionSize("30m")
    someStep1.setTargets(Array(target1))
    val res1 = ConfigProcess.checkAndUpdateAgg(someStep1)
    assert(!res1)

    val someStep2 = new Table()
    val aggCountDep = new Dependency()
    aggCountDep.setType("aggregate")
    aggCountDep.setPartitionCount(7)
    someStep2.setDependencies(Array(aggCountDep))
    val target2 = new Target()
    target2.setPartitionSize("1h")
    someStep2.setTargets(Array(target2))

    val res2 = ConfigProcess.checkAndUpdateAgg(someStep2)
    assert(!res2)

    val someStep3 = new Table()
    val actuallyOneOnOne = new Dependency()
    actuallyOneOnOne.setType("agg")
    actuallyOneOnOne.setPartitionSize("30m")
    someStep3.setDependencies(Array(actuallyOneOnOne))
    someStep3.setTargets(Array(target1))
    val res3 = ConfigProcess.checkAndUpdateAgg(someStep3)
    assert(!res3)

    val someStep4 = new Table()
    val aggDoubleSetDep = new Dependency()
    aggDoubleSetDep.setType("agg")
    aggDoubleSetDep.setPartitionSize("15m")
    aggDoubleSetDep.setPartitionCount(3)
    someStep4.setDependencies(Array(aggDoubleSetDep))
    someStep4.setTargets(Array(target1))
    val res4 = ConfigProcess.checkAndUpdateAgg(someStep4)
    assert(!res4)
  }

  "given correct aggregate dependency, they" should "validate" in {
    val someStep1 = new Table()
    val aggDep = new Dependency()
    aggDep.setType("agg")
    aggDep.setPartitionSize("10m")
    someStep1.setDependencies(Array(aggDep))
    val target1 = new Target()
    target1.setPartitionSize("30m")
    someStep1.setTargets(Array(target1))
    val res1 = ConfigProcess.checkAndUpdateAgg(someStep1)
    assert(res1)

    val someStep2 = new Table()
    val aggCountDep = new Dependency()
    aggCountDep.setType("aggregate")
    aggCountDep.setPartitionCount(5)
    someStep2.setDependencies(Array(aggCountDep))
    val target2 = new Target()
    target2.setPartitionSize("1h")
    someStep2.setTargets(Array(target2))

    val res2 = ConfigProcess.checkAndUpdateAgg(someStep2)
    assert(res2)

    val someStep3 = new Table()
    aggCountDep.setPartitionSize("")
    aggCountDep.setPartitionCount(3)
    someStep3.setDependencies(Array(aggCountDep))
    someStep3.setTargets(Array(target2))

    val res3 = ConfigProcess.checkAndUpdateAgg(someStep3)
    assert(res3)

    val someStep4 = new Table()
    aggCountDep.setPartitionCount(1)
    aggCountDep.setPartitionSize("15m")
    someStep4.setDependencies(Array(aggCountDep))
    someStep4.setTargets(Array(target2))

    val res4 = ConfigProcess.checkAndUpdateAgg(someStep4)
    assert(res4)
  }
}
