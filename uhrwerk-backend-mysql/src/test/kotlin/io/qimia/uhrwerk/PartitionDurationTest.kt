package io.qimia.uhrwerk

import io.qimia.uhrwerk.PartitionDurationTester.PartitionTestDependencyInput
import io.qimia.uhrwerk.PartitionDurationTester.checkDependencies
import io.qimia.uhrwerk.PartitionDurationTester.checkPartitionedDependency
import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Duration

class PartitionDurationTest {
    @Test
    fun identityChecks() {
        val tableDur1 = Duration.ofMinutes(15)
        val depCheck1 = PartitionTestDependencyInput()
        depCheck1.dependencyTableName = "check1"
        depCheck1.transformType = PartitionTransformType.IDENTITY
        depCheck1.dependencyTablePartitionSize = 15
        depCheck1.dependencyTablePartitionUnit = PartitionUnit.MINUTES
        Assertions.assertTrue(checkPartitionedDependency(tableDur1, depCheck1))
        val tableDur2 = Duration.ofHours(1)
        Assertions.assertFalse(checkPartitionedDependency(tableDur2, depCheck1))
        val depCheck2 = PartitionTestDependencyInput()
        depCheck2.dependencyTableName = "check2"
        depCheck2.transformType = PartitionTransformType.IDENTITY
        depCheck2.dependencyTablePartitionSize = 1
        depCheck2.dependencyTablePartitionUnit = PartitionUnit.HOURS
        Assertions.assertTrue(checkPartitionedDependency(tableDur2, depCheck2))
        val tableDur3 = Duration.ofDays((7 * 3).toLong())
        val depCheck3 = PartitionTestDependencyInput()
        depCheck3.dependencyTableName = "check3"
        depCheck3.transformType = PartitionTransformType.IDENTITY
        depCheck3.dependencyTablePartitionSize = 3
        Assertions.assertTrue(checkPartitionedDependency(tableDur3, depCheck3))
    }

    @Test
    fun windowChecks() {
        val tableDur1 = Duration.ofMinutes(20)
        val depCheck1 = PartitionTestDependencyInput()
        depCheck1.dependencyTableName = "check1"
        depCheck1.transformType = PartitionTransformType.WINDOW
        depCheck1.transformSize = 10
        depCheck1.dependencyTablePartitionSize = 20
        depCheck1.dependencyTablePartitionUnit = PartitionUnit.MINUTES
        Assertions.assertTrue(checkPartitionedDependency(tableDur1, depCheck1))
    }

    @Test
    fun aggregateChecks() {
        val tableDur1 = Duration.ofMinutes(30)
        val depCheck1 = PartitionTestDependencyInput()
        depCheck1.dependencyTableName = "check1"
        depCheck1.transformType = PartitionTransformType.AGGREGATE
        depCheck1.transformSize = 2
        depCheck1.dependencyTablePartitionSize = 20
        depCheck1.dependencyTablePartitionUnit = PartitionUnit.MINUTES
        Assertions.assertFalse(checkPartitionedDependency(tableDur1, depCheck1))
        val tableDur2 = Duration.ofMinutes(40)
        Assertions.assertTrue(checkPartitionedDependency(tableDur2, depCheck1))
        val tableDur3 = Duration.ofHours(6)
        val depCheck3 = PartitionTestDependencyInput()
        depCheck3.dependencyTableName = "check3"
        depCheck3.transformType = PartitionTransformType.AGGREGATE
        depCheck3.transformSize = 24
        depCheck3.dependencyTablePartitionSize = 15
        depCheck3.dependencyTablePartitionUnit = PartitionUnit.MINUTES
        Assertions.assertTrue(checkPartitionedDependency(tableDur3, depCheck3))
    }

    @Test
    fun fullDependencyCheck() {
        // Does a full test of three dependencies and a target table with different types of transform
        val outTablePU = PartitionUnit.HOURS
        val outTablePS = 1
        val depA = PartitionTestDependencyInput()
        depA.dependencyTableName = "depA"
        depA.transformType = PartitionTransformType.IDENTITY
        depA.dependencyTablePartitionSize = 60
        depA.dependencyTablePartitionUnit = PartitionUnit.MINUTES
        val depB = PartitionTestDependencyInput()
        depB.dependencyTableName = "depB"
        depB.transformType = PartitionTransformType.WINDOW
        depB.transformSize = 6
        depB.dependencyTablePartitionSize = 1
        depB.dependencyTablePartitionUnit = PartitionUnit.HOURS
        val depC = PartitionTestDependencyInput()
        depC.dependencyTableName = "depC"
        depC.transformType = PartitionTransformType.AGGREGATE
        depC.transformSize = 4
        depC.dependencyTablePartitionSize = 15
        depC.dependencyTablePartitionUnit = PartitionUnit.MINUTES
        val deps = listOf(depA, depB, depC)
        val res = checkDependencies(outTablePU, outTablePS, deps)
        Assertions.assertTrue(res.success)

        // If we change to 3*15 minutes it should not work anymore
        depC.transformSize = 3
        val res2 = checkDependencies(outTablePU, outTablePS, deps)
        Assertions.assertFalse(res2.success)
        Assertions.assertEquals("depC", res2.badTableNames[0])
    }

    @Test
    fun unpartitionedDependencyCheck() {
        val outTablePU = PartitionUnit.HOURS
        val outTablePS = 1
        val depA = PartitionTestDependencyInput()
        depA.dependencyTableName = "depA"
        depA.transformType = PartitionTransformType.IDENTITY
        depA.dependencyTablePartitionSize = 60
        depA.dependencyTablePartitionUnit = PartitionUnit.MINUTES
        val depB = PartitionTestDependencyInput()
        depB.dependencyTableName = "depB"
        depB.transformType = PartitionTransformType.NONE
        depB.dependencyTablePartitionUnit = null
        val deps = listOf(depA, depB)
        val res = checkDependencies(outTablePU, outTablePS, deps)
        Assertions.assertTrue(res.success)

        // Dep defined as partitioned but table is not
        depB.transformType = PartitionTransformType.IDENTITY
        val res2 = checkDependencies(outTablePU, outTablePS, deps)
        Assertions.assertFalse(res2.success)

        // Dep defined as unpartitioned but the table is partitioned
        depB.transformType = PartitionTransformType.NONE
        depB.dependencyTablePartitioned = true
        depB.dependencyTablePartitionUnit = PartitionUnit.HOURS
        depB.dependencyTablePartitionSize = 1
        val res3 = checkDependencies(outTablePU, outTablePS, deps)
        Assertions.assertFalse(res3.success)
    }
}