import io.qimia.uhrwerk.ConnectionHelper
import io.qimia.uhrwerk.common.metastore.config.*
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.config.builders.ConnectionBuilder
import io.qimia.uhrwerk.config.builders.TableBuilder
import org.junit.jupiter.api.*
import java.sql.SQLException
import java.time.LocalDateTime
import java.util.*

/**
 * These tests are similar to the ones in ProcessPartitionTest, but these rely on the other DAO objects to function
 * These also test far more variants than ones in ProcessPartitionTest
 */
class ProcessPartitionIntegrationTest {
    var db: java.sql.Connection? = null
    var tableDAO: TableDAO? = null
    var partitionDAO: PartitionDAO? = null
    var partitionDependencyDAO: PartitionDependencyDAO? = null
    var connectionDAO: ConnectionDAO? = null
    lateinit var singleSourceTestConnection: ConnectionModel
    lateinit var singleDataLakeConnection: ConnectionModel

    @BeforeEach
    @Throws(SQLException::class)
    fun setUp() {
        db = ConnectionHelper.getConnecion()
        tableDAO = TableDAO()
        partitionDAO = PartitionDAO()
        partitionDependencyDAO = PartitionDependencyDAO()
        connectionDAO = ConnectionDAO()
        val connBuilder = ConnectionBuilder()
        singleSourceTestConnection = connBuilder
            .name("testConn")
            .file()
            .path("/some/path/on/disk/")
            .done()
            .build()
        connectionDAO!!.save(singleSourceTestConnection, false)
        singleDataLakeConnection = connBuilder
            .name("testDataLake")
            .file()
            .path("/other/path/on/disk/")
            .done()
            .build()
        connectionDAO!!.save(singleDataLakeConnection, false)
    }

    @AfterEach
    @Throws(SQLException::class)
    fun tearDown() {
        // WARNING deletes all data as cleanup
        val deletePartDependencies = db!!.createStatement() // In case of some lost source data
        deletePartDependencies.execute("DELETE FROM PARTITION_DEPENDENCY")
        deletePartDependencies.close()
        val deletePartitions = db!!.createStatement()
        deletePartitions.execute("DELETE FROM PARTITION_")
        deletePartitions.close()
        val deleteDependencyStm = db!!.createStatement()
        deleteDependencyStm.execute("DELETE FROM DEPENDENCY")
        deleteDependencyStm.close()
        val deleteSourceStm = db!!.createStatement() // In case of some lost source data
        deleteSourceStm.execute("DELETE FROM SOURCE")
        deleteSourceStm.close()
        val deleteTargetStm = db!!.createStatement()
        deleteTargetStm.execute("DELETE FROM TARGET")
        deleteTargetStm.close()
        val deleteConnectionStm = db!!.createStatement()
        deleteConnectionStm.execute("DELETE FROM CONNECTION")
        deleteConnectionStm.close()
        val deleteTableStm = db!!.createStatement()
        deleteTableStm.execute("DELETE FROM TABLE_")
        deleteTableStm.close()
        if (db != null) if (!db!!.isClosed) db!!.close()
    }

    @Test
    @Throws(SQLException::class)
    fun testComplexExample() {
        // First Setup 3 tables to depend on
        val tableBuilderA = TableBuilder()
        val tableA = tableBuilderA
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableA")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .partition()
            .unit("minutes")
            .size(20)
            .done()
            .source()
            .connectionName(singleSourceTestConnection.name)
            .path("/further/disk/path/tableASource")
            .format("parquet")
            .version("testVersion")
            .partition()
            .unit("minutes")
            .size(20)
            .done()
            .select()
            .query("SELECT * FROM <path> WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>';")
            .column("created_at")
            .done()
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        val saveA = tableDAO!!.save(tableA, false)
        Assertions.assertTrue(saveA.isSuccess)
        val tableBuilderB = TableBuilder()
        val tableBPathName = "/further/disk/path/tableBSource"
        val tableB = tableBuilderB
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableB")
            .version("testVersion")
            .parallelism(4)
            .maxBulkSize(8)
            .partition()
            .unit("minutes")
            .size(20)
            .done()
            .source()
            .connectionName(singleSourceTestConnection.name)
            .path(tableBPathName)
            .format("parquet")
            .version("testVersion")
            .partition()
            .unit("minutes")
            .size(20)
            .done()
            .select()
            .query("SELECT * FROM <path> WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>';")
            .column("created_at")
            .done()
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        Assertions.assertEquals(tableBPathName, tableB.sources!![0].path)
        val saveB = tableDAO!!.save(tableB, false)
        Assertions.assertTrue(saveB.isSuccess)
        val tableBuilderC = TableBuilder()
        val tableC = tableBuilderC
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableC")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1) // Note no partition information
            .source()
            .connectionName(singleSourceTestConnection.name)
            .path("/further/disk/path/tableCSource")
            .format("parquet")
            .version("testVersion") // And no partitioning for it's source
            .select()
            .query("SELECT * FROM <path>;")
            .column("FIXME") // TODO: Should not be necessary for unpartitioned tables
            .done()
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        Assertions.assertFalse(tableC.partitioned)
        val saveC = tableDAO!!.save(tableC, false)
        Assertions.assertTrue(saveC.isSuccess)
        val tableBuilderOut = TableBuilder()
        val outTable = tableBuilderOut
            .area("test_out_area")
            .vertical("test_vertical")
            .table("tableOut")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .partition()
            .unit("minutes")
            .size(20)
            .done()
            .dependency()
            .area(tableA.area)
            .vertical(tableA.vertical)
            .table(tableA.name)
            .version(tableA.version)
            .format("parquet")
            .transform()
            .type("identity")
            .done()
            .done()
            .dependency()
            .area(tableB.area)
            .vertical(tableB.vertical)
            .table(tableB.name)
            .version(tableB.version)
            .format("parquet")
            .transform()
            .type("window")
            .partition()
            .size(3)
            .done()
            .done()
            .done()
            .dependency()
            .area(tableC.area)
            .vertical(tableC.vertical)
            .table(tableC.name)
            .version(tableC.version)
            .format("parquet")
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        val saveOut = tableDAO!!.save(outTable, false)
        Assertions.assertTrue(saveOut.isSuccess)
        val requestedTime = LocalDateTime.of(2020, 4, 10, 14, 0)

        // First process with no partitions written
        val res1 = tableDAO!!.processingPartitions(outTable, listOf(requestedTime))
        Assertions.assertEquals(1, res1.failedTs.size)
        Assertions.assertEquals(0, res1.resolved.size)
        Assertions.assertEquals(0, res1.processed.size)
        val res1n1 = res1.failed[0]
        Assertions.assertEquals(0, res1n1.resolvedDependencies.size)
        Assertions.assertEquals(3, res1n1.failedDependencies.size)
        Assertions.assertEquals(requestedTime, res1n1.partitionTs)
        Assertions.assertFalse(res1n1.isProcessed)
        Assertions.assertFalse(res1n1.isResolved)

        // Next we write the partition for table A
        val partA = Partition()
        partA.partitioned = true
        partA.partitionSize = 20
        partA.partitionUnit = PartitionUnit.MINUTES
        partA.partitionTs = requestedTime
        partA.targetId = tableA.targets!![0].id
        val savePartA = partitionDAO!!.save(partA, false)
        Assertions.assertTrue(savePartA.isSuccess)

        // And check if that results in a changed test
        val res2 = tableDAO!!.processingPartitions(outTable, listOf(requestedTime))
        Assertions.assertEquals(1, res2.failedTs.size)
        Assertions.assertEquals(0, res2.resolved.size)
        Assertions.assertEquals(0, res2.processed.size)
        val res2n1 = res2.failed[0]
        val resolvedDeps2 = res2n1.resolvedDependencies
        Assertions.assertEquals(1, resolvedDeps2.size)
        Assertions.assertEquals(tableA.name, resolvedDeps2[0].dependency.tableName)
        Assertions.assertTrue(resolvedDeps2[0].isSuccess)
        Assertions.assertEquals(2, res2n1.failedDependencies.size)
        Assertions.assertEquals(requestedTime, res2n1.partitionTs)
        Assertions.assertFalse(res2n1.isProcessed)
        Assertions.assertFalse(res2n1.isResolved)

        // Next write some old snapshot for table C
        val oldSnapshotPartC = Partition()
        val oldTime = LocalDateTime.of(2000, 6, 6, 10, 0)
        oldSnapshotPartC.partitioned = false
        oldSnapshotPartC.partitionTs = oldTime
        oldSnapshotPartC.targetId = tableC.targets!![0].id
        val saveOldPartC = partitionDAO!!.save(oldSnapshotPartC, false)
        Assertions.assertTrue(saveOldPartC.isSuccess)

        // and check if this old datetime can be extracted from the failed dependencies
        val res3 = tableDAO!!.processingPartitions(outTable, listOf(requestedTime))
        Assertions.assertEquals(1, res3.failedTs.size)
        val res3n1 = res3.failed[0]
        Assertions.assertEquals(1, res3n1.resolvedDependencies.size)
        val res3FailedDeps = res3n1.failedDependencies
            .filter { x: DependencyResult -> x.dependency.tableName == tableC.name }
        Assertions.assertEquals(oldTime, res3FailedDeps[0].failed[0])

        // Now write a new (up2date) snapshot for table c
        val newSnapshotPartC = Partition()
        val newTime = LocalDateTime.now()
        newSnapshotPartC.partitioned = false
        newSnapshotPartC.partitionTs = newTime
        newSnapshotPartC.targetId = tableC.targets!![0].id
        val saveNewPartC = partitionDAO!!.save(newSnapshotPartC, false)
        Assertions.assertTrue(saveNewPartC.isSuccess)

        // and check to see if now 2 out of 3 dependencies are met
        val res4 = tableDAO!!.processingPartitions(outTable, listOf(requestedTime))
        Assertions.assertEquals(1, res4.failedTs.size)
        val res4n1 = res4.failed[0]
        Assertions.assertEquals(2, res4n1.resolvedDependencies.size)

        // Now only fill in a single partition of the windowed dependency
        val partB = Partition()
        partB.partitioned = true
        partB.partitionSize = 20
        partB.partitionUnit = PartitionUnit.MINUTES
        partB.partitionTs = requestedTime
        partB.targetId = tableB.targets!![0].id
        val savePartB = partitionDAO!!.save(partB, false)
        Assertions.assertTrue(savePartB.isSuccess)

        // Check that that didn't change anything for the checker
        val res5 = tableDAO!!.processingPartitions(outTable, listOf(requestedTime))
        Assertions.assertEquals(1, res5.failedTs.size)
        val res5n1 = res5.failed[0]
        Assertions.assertEquals(2, res5n1.resolvedDependencies.size)

        // Fill the 2 other partitions required for the window
        val partBWindow1 = Partition()
        partBWindow1.partitioned = true
        partBWindow1.partitionSize = 20
        partBWindow1.partitionUnit = PartitionUnit.MINUTES
        partBWindow1.partitionTs = LocalDateTime.of(2020, 4, 10, 13, 40)
        partBWindow1.targetId = tableB.targets!![0].id
        val partBWindow2 = Partition()
        partBWindow2.partitioned = true
        partBWindow2.partitionSize = 20
        partBWindow2.partitionUnit = PartitionUnit.MINUTES
        partBWindow2.partitionTs = LocalDateTime.of(2020, 4, 10, 13, 20)
        partBWindow2.targetId = tableB.targets!![0].id
        val savePartBWindow = partitionDAO!!.save(listOf(partBWindow1, partBWindow2), false)
        savePartBWindow.forEach { x: PartitionResult -> Assertions.assertTrue(x.isSuccess) }

        // Now it should be ready to process
        val res6 = tableDAO!!.processingPartitions(outTable, listOf(requestedTime))
        Assertions.assertEquals(1, res6.resolved.size)
        val res6n1 = res6.resolved[0]
        Assertions.assertEquals(3, res6n1.resolvedDependencies.size)

        // Add a partition denoting it has been processed
        val outPart = Partition()
        outPart.partitioned = true
        outPart.partitionSize = 20
        outPart.partitionUnit = PartitionUnit.MINUTES
        outPart.partitionTs = requestedTime
        outPart.targetId = outTable.targets!![0].id
        val saveOutPart = partitionDAO!!.save(outPart, false)
        Assertions.assertTrue(saveOutPart.isSuccess)

        // check if it shows if it has been processed already
        val res7 = tableDAO!!.processingPartitions(outTable, listOf(requestedTime))
        Assertions.assertEquals(1, res7.processed.size)
    }

    @Test
    @Throws(SQLException::class)
    fun testComplexExample2() {
        val tableBuilderA = TableBuilder()
        val tableA = tableBuilderA
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableA")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .partition()
            .unit("hours")
            .size(6)
            .done()
            .source()
            .connectionName(singleSourceTestConnection.name)
            .path("/further/disk/path/tableASource")
            .format("parquet")
            .version("testVersion")
            .partition()
            .unit("minutes")
            .size(20)
            .done()
            .select()
            .query("SELECT * FROM <path> WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>';")
            .column("created_at")
            .done()
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        val saveA = tableDAO!!.save(tableA, false)
        Assertions.assertTrue(saveA.isSuccess)
        val tableBuilderB = TableBuilder()
        val tableB = tableBuilderB
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableB")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .source()
            .connectionName(singleSourceTestConnection.name)
            .path("/further/disk/path/tableBSource")
            .format("parquet")
            .version("testVersion")
            .select()
            .query("SELECT * FROM <path>;")
            .column("FIXME") // TODO: Should not be necessary for unpartitioned tables
            .done()
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        val saveB = tableDAO!!.save(tableB, false)
        Assertions.assertTrue(saveB.isSuccess)
        val tableBuilderOut = TableBuilder()
        val outTable = tableBuilderOut
            .area("test_out_area")
            .vertical("test_vertical")
            .table("tableOut")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .partition()
            .unit("days")
            .size(1)
            .done()
            .dependency()
            .area(tableA.area)
            .vertical(tableA.vertical)
            .table(tableA.name)
            .version(tableA.version)
            .format("parquet")
            .transform()
            .type("aggregate")
            .partition()
            .size(4)
            .done()
            .done()
            .done()
            .dependency()
            .area(tableB.area)
            .vertical(tableB.vertical)
            .table(tableB.name)
            .version(tableB.version)
            .format("parquet")
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        // Test that it's only allowed to save if the partitionSize matches up with the dependency
        val badOutTable = tableBuilderOut.build()
        badOutTable.partitionSize = 8
        val badSaveOut = tableDAO!!.save(badOutTable, false)
        Assertions.assertFalse(badSaveOut.isSuccess)
        val saveOut = tableDAO!!.save(outTable, true)
        Assertions.assertTrue(saveOut.isSuccess)
        val requestedTimes = listOf(
            LocalDateTime.of(2020, 2, 10, 0, 0),
            LocalDateTime.of(2020, 2, 11, 0, 0),
            LocalDateTime.of(2020, 2, 12, 0, 0)
        )

        // First check is without any partitions filled
        val res1 = tableDAO!!.processingPartitions(outTable, requestedTimes)
        Assertions.assertEquals(3, res1.failed.size)
        Assertions.assertEquals(0, res1.resolved.size)
        Assertions.assertEquals(0, res1.processed.size)

        // Now fill in some of dependencyA's partitions
        val dependencyTimes2 = listOf(
            LocalDateTime.of(2020, 2, 10, 0, 0),
            LocalDateTime.of(2020, 2, 10, 6, 0),
            LocalDateTime.of(2020, 2, 10, 12, 0),
            LocalDateTime.of(2020, 2, 10, 18, 0)
        )
        for (depATime in dependencyTimes2) {
            val partA = Partition()
            partA.partitioned = true
            partA.partitionSize = 6
            partA.partitionUnit = PartitionUnit.HOURS
            partA.partitionTs = depATime
            partA.targetId = tableA.targets!![0].id
            val savePartA = partitionDAO!!.save(partA, false)
            Assertions.assertTrue(savePartA.isSuccess)
        }

        // This check should show that for the first localdatetime, dependency A has been met
        val res2 = tableDAO!!.processingPartitions(outTable, requestedTimes)
        val res2failed = res2.failed
        Assertions.assertEquals(3, res2failed.size)
        Assertions.assertEquals(1, res2failed[0].resolvedDependencies.size)

        // Now add a snapshot-partition for table-b
        val newSnapshotPartB = Partition()
        val newTime = LocalDateTime.now()
        newSnapshotPartB.partitioned = false
        newSnapshotPartB.partitionTs = newTime
        newSnapshotPartB.targetId = tableB.targets!![0].id
        val saveNewPartB = partitionDAO!!.save(newSnapshotPartB, false)
        Assertions.assertTrue(saveNewPartB.isSuccess)

        // And the first partition should show success
        val res3 = tableDAO!!.processingPartitions(outTable, requestedTimes)
        Assertions.assertEquals(2, res3.failed.size)
        Assertions.assertEquals(1, res3.resolved.size)

        // Filling the outTable for the first partition
        val outPart = Partition()
        outPart.partitioned = true
        outPart.partitionSize = 1
        outPart.partitionUnit = PartitionUnit.DAYS
        outPart.partitionTs = requestedTimes[0]
        outPart.targetId = outTable.targets!![0].id
        val saveOutPart = partitionDAO!!.save(outPart, false)
        Assertions.assertTrue(saveOutPart.isSuccess)

        // and the first partition should show processed
        val res4 = tableDAO!!.processingPartitions(outTable, requestedTimes)
        Assertions.assertEquals(2, res4.failed.size)
        Assertions.assertEquals(1, res4.processed.size)

        // fill the rest of dependency A save for 1
        val dependencyTimes5 = listOf(
            LocalDateTime.of(2020, 2, 11, 0, 0),
            LocalDateTime.of(2020, 2, 11, 6, 0),
            LocalDateTime.of(2020, 2, 11, 12, 0),
            LocalDateTime.of(2020, 2, 11, 18, 0),
            LocalDateTime.of(2020, 2, 12, 0, 0),  // Skip one here
            LocalDateTime.of(2020, 2, 12, 12, 0),
            LocalDateTime.of(2020, 2, 12, 18, 0)
        )
        for (depATime in dependencyTimes5) {
            val partA = Partition()
            partA.partitioned = true
            partA.partitionSize = 6
            partA.partitionUnit = PartitionUnit.HOURS
            partA.partitionTs = depATime
            partA.targetId = tableA.targets!![0].id
            val savePartA = partitionDAO!!.save(partA, false)
            Assertions.assertTrue(savePartA.isSuccess)
        }
        val res5 = tableDAO!!.processingPartitions(outTable, requestedTimes)
        Assertions.assertEquals(1, res5.resolved.size)
        Assertions.assertEquals(1, res5.failed.size)
        Assertions.assertEquals(1, res5.processed.size)
        Assertions.assertEquals(LocalDateTime.of(2020, 2, 12, 0, 0), res5.failedTs[0])
    }

    @Test
    @Throws(SQLException::class)
    fun testPartitionlessTableDependency() {
        val tableBuilderA = TableBuilder()
        val tableA = tableBuilderA
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableA")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .source()
            .connectionName(singleSourceTestConnection.name)
            .path("/further/disk/path/tableASource")
            .format("parquet")
            .version("testVersion")
            .select()
            .query("SELECT * FROM <path>;")
            .column("FIXME") // TODO: Should not be necessary for unpartitioned tables
            .done()
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        val saveA = tableDAO!!.save(tableA, false)
        Assertions.assertTrue(saveA.isSuccess)
        val tableBuilderOut = TableBuilder()
        val outTable = tableBuilderOut
            .area("test_out_area")
            .vertical("test_vertical")
            .table("tableOut")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .dependency()
            .area(tableA.area)
            .vertical(tableA.vertical)
            .table(tableA.name)
            .version(tableA.version)
            .format("parquet")
            .done()
            .target()
            .connectionName(singleDataLakeConnection.name)
            .format("parquet")
            .done()
            .build()
        val saveOut = tableDAO!!.save(outTable, true)
        Assertions.assertTrue(saveOut.isSuccess)

        // Test without having the dependency ready
        val requestTime = LocalDateTime.now()
        val res1 = tableDAO!!.processingPartitions(outTable, listOf(requestTime))
        Assertions.assertEquals(1, res1.failed.size)

        // Add dependency partition
        val newSnapshotPartA = Partition()
        newSnapshotPartA.partitioned = false
        newSnapshotPartA.partitionTs = requestTime
        newSnapshotPartA.targetId = tableA.targets!![0].id
        val saveNewPartB = partitionDAO!!.save(newSnapshotPartA, false)
        Assertions.assertTrue(saveNewPartB.isSuccess)

        // Check again, it should be ready
        val requestTime2 = LocalDateTime.now()
        val res2 = tableDAO!!.processingPartitions(outTable, listOf(requestTime2))
        Assertions.assertEquals(1, res2.resolved.size)

        // Add a new output partition
        val newSnapshotOutPart = Partition()
        newSnapshotOutPart.partitioned = false
        newSnapshotOutPart.partitionTs = requestTime2
        newSnapshotOutPart.targetId = outTable.targets!![0].id
        val saveOutPart = partitionDAO!!.save(newSnapshotOutPart, false)
        Assertions.assertTrue(saveOutPart.isSuccess)

        // Check for this new request time
        val res3 = tableDAO!!.processingPartitions(outTable, listOf(requestTime2))
        Assertions.assertEquals(1, res3.processed.size)
        // TODO: At the moment it will still say processed if requested for the same minute
    }
}