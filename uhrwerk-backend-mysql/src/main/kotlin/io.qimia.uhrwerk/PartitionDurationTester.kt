package io.qimia.uhrwerk

import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import org.slf4j.LoggerFactory
import java.time.Duration

object PartitionDurationTester {
    private val logger = LoggerFactory.getLogger(PartitionDurationTester::class.java)

    /**
     * Convert a PartitionUnit + size pair (denoting a length of time) to a single java Duration
     * object
     *
     * @param u Partition Unit enum
     * @param size amount of partition unit
     * @return a Duration object which can be compared
     */
    fun convertToDuration(u: PartitionUnit?, size: Int): Duration {
        var partitionSize = Duration.ZERO
        when (u) {
            PartitionUnit.WEEKS -> partitionSize = Duration.ofDays((7 * size).toLong())
            PartitionUnit.DAYS -> partitionSize = Duration.ofDays(size.toLong())
            PartitionUnit.HOURS -> partitionSize = Duration.ofHours(size.toLong())
            PartitionUnit.MINUTES -> partitionSize = Duration.ofMinutes(size.toLong())
            else -> logger.error("Unsupported partitionunit given")
        }
        return partitionSize
    }

    /**
     * Check for a single partitioned dependency if the size is correctly set or not
     *
     * @param tablePartitionSize partition size of the table's partition
     * @param testObj Dependency's partitioning information
     * @return true if they match up, false if not
     */
    @JvmStatic
    fun checkPartitionedDependency(
        tablePartitionSize: Duration, testObj: PartitionTestDependencyInput
    ): Boolean {
        if (testObj.dependencyTablePartitionUnit == null) {
            return false
        }
        val dependencyTableSize = convertToDuration(
            testObj.dependencyTablePartitionUnit, testObj.dependencyTablePartitionSize
        )
        if (testObj.transformType == PartitionTransformType.IDENTITY || testObj.transformType == PartitionTransformType.WINDOW) {
            return dependencyTableSize == tablePartitionSize
        } else if (testObj.transformType == PartitionTransformType.AGGREGATE) {
            val minutesDividedOfTable = tablePartitionSize.toMinutes() / testObj.transformSize
            return dependencyTableSize == Duration.ofMinutes(minutesDividedOfTable)
        }
        return false
    }

    /**
     * Check for a single unpartitioned dependency if the table is actually unpartitioned
     *
     * @param testObj Dependency's partitioning information
     * @return true if they match up, false if not
     */
    fun checkUnpartitionedDependency(testObj: PartitionTestDependencyInput): Boolean {
        assert(testObj.transformType == PartitionTransformType.NONE) { "Not an unpartitioned dependency" }
        return !testObj.dependencyTablePartitioned
    }

    /**
     * Check for an array of Dependencies (with included Table partition information) if they are
     * correct for a given target table's partition size. This only works for partitioned tables.
     *
     * @param tablePartitionUnit Unit of time for denoting the target table's partition size
     * @param tablePartitionSize Count denoting how many PartitionUnit makes up the target table's
     * partition size
     * @param dependencies All the dependencies' transitions + the partition information from their
     * tables
     * @return result showing if it was correct and if not names the tables which are not agreeing
     * with the table (target)
     */
    @JvmStatic
    fun checkDependencies(
        tablePartitionUnit: PartitionUnit?,
        tablePartitionSize: Int,
        dependencies: List<PartitionTestDependencyInput>
    ): PartitionTestResult {
        val result = PartitionTestResult()
        result.success = true
        result.badTableNames = mutableListOf()
        val badTables = mutableListOf<String>()

        val tableDuration = convertToDuration(tablePartitionUnit, tablePartitionSize)

        for (testDependency in dependencies) {
            var checkRes: Boolean = if (testDependency.transformType == PartitionTransformType.NONE) {
                checkUnpartitionedDependency(testDependency)
            } else {
                checkPartitionedDependency(tableDuration, testDependency)
            }
            if (!checkRes) {
                badTables.add(testDependency.dependencyTableName!!)
            }
            // if it is an unpartitioned dependency then existing is enough
            // (and any partitioned table can be loaded as an unpartitioned one)
        }
        if (!badTables.isEmpty()) {
            result.success = false
            result.badTableNames = badTables
        }
        return result
    }

    /**
     * Result object for checking dependencies If a test fails, it will show which tables are the
     * cause of the test failing
     */
    class PartitionTestResult {
        @JvmField
        var success = false

        @JvmField
        var badTableNames: List<String?> = mutableListOf()
    }

    /**
     * Input for checking dependency partition sizes contains info about the dependency's
     * transformation + info about the partition size of the table its dependending on
     */
    class PartitionTestDependencyInput {
        @JvmField
        var dependencyTableName: String? = null

        @JvmField
        var dependencyTablePartitioned = false

        @JvmField
        var dependencyTablePartitionUnit: PartitionUnit? = null

        @JvmField
        var dependencyTablePartitionSize = 0

        @JvmField
        var transformType: PartitionTransformType? = null

        @JvmField
        var transformSize = 0
    }
}