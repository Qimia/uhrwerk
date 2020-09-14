package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.ConnectionHelper;
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResult;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.Partition;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.common.model.Table;
import io.qimia.uhrwerk.config.ConnectionBuilder;
import io.qimia.uhrwerk.config.PartitionBuilder;
import io.qimia.uhrwerk.config.SourceBuilder;
import io.qimia.uhrwerk.config.TableBuilder;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.stream.Collector;

import static org.junit.jupiter.api.Assertions.*;

/**
 * These tests are similar to the ones in ProcessPartitionTest, but these rely on the other DAO objects to function
 * These also test far more variants than ones in ProcessPartitionTest
 */
public class ProcessPartitionIntegrationTest {

    java.sql.Connection db;

    TableDAO tableDAO;
    PartitionDAO partitionDAO;
    PartitionDependencyDAO partitionDependencyDAO;
    ConnectionDAO connectionDAO;

    Connection singleSourceTestConnection;
    Connection singleDataLakeConnection;

    @BeforeEach
    void setUp() throws SQLException {
        db = ConnectionHelper.getConnection();
        tableDAO = new TableDAO(db);
        partitionDAO = new PartitionDAO(db);
        partitionDependencyDAO = new PartitionDependencyDAO(db);
        connectionDAO = new ConnectionDAO(db);

        var connBuilder = new ConnectionBuilder();
        singleSourceTestConnection = connBuilder
                .name("testConn")
                .file()
                    .path("/some/path/on/disk/")
                    .done()
                .build();
        connectionDAO.save(singleSourceTestConnection);
        singleDataLakeConnection = connBuilder
                .name("testDataLake")
                .file()
                    .path("/other/path/on/disk/")
                    .done()
                .build();
        connectionDAO.save(singleDataLakeConnection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        // WARNING deletes all data as cleanup
        var deletePartDependencies = db.createStatement(); // In case of some lost source data
        deletePartDependencies.execute("DELETE FROM PARTITION_DEPENDENCY");
        deletePartDependencies.close();
        var deletePartitions = db.createStatement();
        deletePartitions.execute("DELETE FROM PARTITION_");
        deletePartitions.close();
        var deleteDependencyStm = db.createStatement();
        deleteDependencyStm.execute("DELETE FROM DEPENDENCY");
        deleteDependencyStm.close();
        var deleteSourceStm = db.createStatement(); // In case of some lost source data
        deleteSourceStm.execute("DELETE FROM SOURCE");
        deleteSourceStm.close();
        var deleteTargetStm = db.createStatement();
        deleteTargetStm.execute("DELETE FROM TARGET");
        deleteTargetStm.close();
        var deleteConnectionStm = db.createStatement();
        deleteConnectionStm.execute("DELETE FROM CONNECTION");
        deleteConnectionStm.close();
        var deleteTableStm = db.createStatement();
        deleteTableStm.execute("DELETE FROM TABLE_");
        deleteTableStm.close();
        if (db != null) if (!db.isClosed()) db.close();
    }


    @Test
    void testComplexExample() throws SQLException {
        // First Setup 3 tables to depend on
        var tableBuilderA = new TableBuilder();
        Table tableA = tableBuilderA
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
                .connectionName(singleSourceTestConnection.getName())
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
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        var saveA = tableDAO.save(tableA, false);
        assertTrue(saveA.isSuccess());

        var tableBuilderB = new TableBuilder();

        String tableBPathName = "/further/disk/path/tableBSource";
        Table tableB = tableBuilderB
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
                .connectionName(singleSourceTestConnection.getName())
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
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        assertEquals(tableBPathName, tableB.getSources()[0].getPath());
        var saveB = tableDAO.save(tableB, false);
        assertTrue(saveB.isSuccess());

        var tableBuilderC = new TableBuilder();

        Table tableC = tableBuilderC
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableC")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1) // Note no partition information
            .source()
                .connectionName(singleSourceTestConnection.getName())
                .path("/further/disk/path/tableCSource")
                .format("parquet")
                .version("testVersion") // And no partitioning for it's source
                .select()
                    .query("SELECT * FROM <path>;")
                    .column("FIXME")    // TODO: Should not be necessary for unpartitioned tables
                    .done()
                .done()
            .target()
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        assertFalse(tableC.isPartitioned());
        var saveC = tableDAO.save(tableC, false);
        assertTrue(saveC.isSuccess());

        var tableBuilderOut = new TableBuilder();

        Table outTable = tableBuilderOut
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
                .area(tableA.getArea())
                .vertical(tableA.getVertical())
                .table(tableA.getName())
                .version(tableA.getVersion())
                .format("parquet")
                .transform()
                    .type("identity")
                    .done()
                .done()
            .dependency()
                .area(tableB.getArea())
                .vertical(tableB.getVertical())
                .table(tableB.getName())
                .version(tableB.getVersion())
                .format("parquet")
                .transform()
                    .type("window")
                    .partition()
                        .size(3)
                        .done()
                    .done()
                .done()
            .dependency()
                .area(tableC.getArea())
                .vertical(tableC.getVertical())
                .table(tableC.getName())
                .version(tableC.getVersion())
                .format("parquet")
                .done()
            .target()
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        var saveOut = tableDAO.save(outTable, false);
        assertTrue(saveOut.isSuccess());


        LocalDateTime requestedTime = LocalDateTime.of(2020, 4, 10, 14, 0);

        // First process with no partitions written
        TablePartitionResultSet res1 = tableDAO.processingPartitions(outTable, new LocalDateTime[] {requestedTime});
        assertEquals(1, res1.getFailedTs().length);
        assertEquals(0, res1.getResolved().length);
        assertEquals(0, res1.getProcessed().length);
        TablePartitionResult res1n1 = res1.getFailed()[0];

        assertEquals(0, res1n1.getResolvedDependencies().length);
        assertEquals(3, res1n1.getFailedDependencies().length);
        assertEquals(requestedTime, res1n1.getPartitionTs());
        assertFalse(res1n1.isProcessed());
        assertFalse(res1n1.isResolved());

        // Next we write the partition for table A
        var partA = new Partition();
        partA.setPartitioned(true);
        partA.setPartitionSize(20);
        partA.setPartitionUnit(PartitionUnit.MINUTES);
        partA.setPartitionTs(requestedTime);
        partA.setTargetId(tableA.getTargets()[0].getId());
        partA.setKey();
        var savePartA = partitionDAO.save(partA, false);
        assertTrue(savePartA.isSuccess());

        // And check if that results in a changed test
        TablePartitionResultSet res2 = tableDAO.processingPartitions(outTable, new LocalDateTime[] {requestedTime});
        assertEquals(1, res2.getFailedTs().length);
        assertEquals(0, res2.getResolved().length);
        assertEquals(0, res2.getProcessed().length);
        TablePartitionResult res2n1 = res2.getFailed()[0];
        var resolvedDeps2 = res2n1.getResolvedDependencies();
        assertEquals(1, resolvedDeps2.length);
        assertEquals(tableA.getName(), resolvedDeps2[0].getDependency().getTableName());
        assertTrue(resolvedDeps2[0].isSuccess());
        assertEquals(2, res2n1.getFailedDependencies().length);
        assertEquals(requestedTime, res2n1.getPartitionTs());
        assertFalse(res2n1.isProcessed());
        assertFalse(res2n1.isResolved());

        // Next write some old snapshot for table C
        var oldSnapshotPartC = new Partition();
        LocalDateTime oldTime = LocalDateTime.of(2000, 6, 6, 10, 0);
        oldSnapshotPartC.setPartitioned(false);
        oldSnapshotPartC.setPartitionTs(oldTime);
        oldSnapshotPartC.setTargetId(tableC.getTargets()[0].getId());
        oldSnapshotPartC.setKey();
        var saveOldPartC = partitionDAO.save(oldSnapshotPartC, false);
        assertTrue(saveOldPartC.isSuccess());

        // and check if this old datetime can be extracted from the failed dependencies
        TablePartitionResultSet res3 =
            tableDAO.processingPartitions(outTable, new LocalDateTime[] {requestedTime});
        assertEquals(1, res3.getFailedTs().length);
        TablePartitionResult res3n1 = res3.getFailed()[0];
        assertEquals(1, res3n1.getResolvedDependencies().length);
        DependencyResult[] res3FailedDeps =
            Arrays.stream(res3n1.getFailedDependencies())
                .filter(x -> x.getDependency().getTableName().equals(tableC.getName()))
                .toArray(DependencyResult[]::new);
        assertEquals(oldTime, res3FailedDeps[0].getFailed()[0]);

        // Now write a new (up2date) snapshot for table c
        var newSnapshotPartC = new Partition();
        LocalDateTime newTime = LocalDateTime.now();
        newSnapshotPartC.setPartitioned(false);
        newSnapshotPartC.setPartitionTs(newTime);
        newSnapshotPartC.setTargetId(tableC.getTargets()[0].getId());
        newSnapshotPartC.setKey();
        var saveNewPartC = partitionDAO.save(newSnapshotPartC, false);
        assertTrue(saveNewPartC.isSuccess());

        // and check to see if now 2 out of 3 dependencies are met
        TablePartitionResultSet res4 =
                tableDAO.processingPartitions(outTable, new LocalDateTime[] {requestedTime});
        assertEquals(1, res4.getFailedTs().length);
        TablePartitionResult res4n1 = res4.getFailed()[0];
        assertEquals(2, res4n1.getResolvedDependencies().length);

        // Now only fill in a single partition of the windowed dependency
        var partB = new Partition();
        partB.setPartitioned(true);
        partB.setPartitionSize(20);
        partB.setPartitionUnit(PartitionUnit.MINUTES);
        partB.setPartitionTs(requestedTime);
        partB.setTargetId(tableB.getTargets()[0].getId());
        partB.setKey();
        var savePartB = partitionDAO.save(partB, false);
        assertTrue(savePartB.isSuccess());

        // Check that that didn't change anything for the checker
        TablePartitionResultSet res5 =
                tableDAO.processingPartitions(outTable, new LocalDateTime[] {requestedTime});
        assertEquals(1, res5.getFailedTs().length);
        TablePartitionResult res5n1 = res5.getFailed()[0];
        assertEquals(2, res5n1.getResolvedDependencies().length);

        // Fill the 2 other partitions required for the window
        var partBWindow1 = new Partition();
        partBWindow1.setPartitioned(true);
        partBWindow1.setPartitionSize(20);
        partBWindow1.setPartitionUnit(PartitionUnit.MINUTES);
        partBWindow1.setPartitionTs(LocalDateTime.of(2020, 4, 10, 13, 40));
        partBWindow1.setTargetId(tableB.getTargets()[0].getId());
        partBWindow1.setKey();
        var partBWindow2 = new Partition();
        partBWindow2.setPartitioned(true);
        partBWindow2.setPartitionSize(20);
        partBWindow2.setPartitionUnit(PartitionUnit.MINUTES);
        partBWindow2.setPartitionTs(LocalDateTime.of(2020, 4, 10, 13, 20));
        partBWindow2.setTargetId(tableB.getTargets()[0].getId());
        partBWindow2.setKey();
        var savePartBWindow = partitionDAO.save(new Partition[]{partBWindow1, partBWindow2}, false);
        Arrays.stream(savePartBWindow).forEach(x -> assertTrue(x.isSuccess()));

        // Now it should be ready to process
        TablePartitionResultSet res6 =
                tableDAO.processingPartitions(outTable, new LocalDateTime[] {requestedTime});
        assertEquals(1, res6.getResolved().length);
        TablePartitionResult res6n1 = res6.getResolved()[0];
        assertEquals(3, res6n1.getResolvedDependencies().length);

        // Add a partition denoting it has been processed
        var outPart = new Partition();
        outPart.setPartitioned(true);
        outPart.setPartitionSize(20);
        outPart.setPartitionUnit(PartitionUnit.MINUTES);
        outPart.setPartitionTs(requestedTime);
        outPart.setTargetId(outTable.getTargets()[0].getId());
        outPart.setKey();
        var saveOutPart = partitionDAO.save(outPart, false);
        assertTrue(saveOutPart.isSuccess());

        // check if it shows if it has been processed already
        TablePartitionResultSet res7 =
                tableDAO.processingPartitions(outTable, new LocalDateTime[] {requestedTime});
        assertEquals(1, res7.getProcessed().length);
    }

    @Test
    void testComplexExample2() throws SQLException {
        var tableBuilderA = new TableBuilder();
        Table tableA = tableBuilderA
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
                .connectionName(singleSourceTestConnection.getName())
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
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        var saveA = tableDAO.save(tableA, false);
        assertTrue(saveA.isSuccess());

        var tableBuilderB = new TableBuilder();
        Table tableB = tableBuilderB
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableB")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .source()
                .connectionName(singleSourceTestConnection.getName())
                .path("/further/disk/path/tableBSource")
                .format("parquet")
                .version("testVersion")
                .select()
                    .query("SELECT * FROM <path>;")
                    .column("FIXME")    // TODO: Should not be necessary for unpartitioned tables
                    .done()
                .done()
            .target()
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        var saveB = tableDAO.save(tableB, false);
        assertTrue(saveB.isSuccess());

        var tableBuilderOut = new TableBuilder();
        Table outTable = tableBuilderOut
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
                .area(tableA.getArea())
                .vertical(tableA.getVertical())
                .table(tableA.getName())
                .version(tableA.getVersion())
                .format("parquet")
                .transform()
                    .type("aggregate")
                    .partition()
                        .size(4)
                        .done()
                    .done()
                .done()
            .dependency()
                .area(tableB.getArea())
                .vertical(tableB.getVertical())
                .table(tableB.getName())
                .version(tableB.getVersion())
                .format("parquet")
                .done()
            .target()
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        // Test that it's only allowed to save if the partitionSize matches up with the dependency
        var badOutTable = tableBuilderOut.build();
        badOutTable.setPartitionSize(8);
        var badSaveOut = tableDAO.save(badOutTable, false);
        assertFalse(badSaveOut.isSuccess());

        var saveOut = tableDAO.save(outTable, true);
        assertTrue(saveOut.isSuccess());

        LocalDateTime[] requestedTimes = new LocalDateTime[] {
            LocalDateTime.of(2020, 2, 10, 0, 0),
            LocalDateTime.of(2020, 2, 11, 0, 0),
            LocalDateTime.of(2020, 2, 12, 0, 0)
        };

        // First check is without any partitions filled
        TablePartitionResultSet res1 = tableDAO.processingPartitions(outTable, requestedTimes);
        assertEquals(3, res1.getFailed().length);
        assertEquals(0, res1.getResolved().length);
        assertEquals(0, res1.getProcessed().length);

        // Now fill in some of dependencyA's partitions
        LocalDateTime[] dependencyTimes2 = new LocalDateTime[] {
            LocalDateTime.of(2020, 2, 10, 0, 0),
            LocalDateTime.of(2020, 2, 10, 6, 0),
            LocalDateTime.of(2020, 2, 10, 12, 0),
            LocalDateTime.of(2020, 2, 10, 18, 0),
        };
        for (LocalDateTime depATime: dependencyTimes2) {
            var partA = new Partition();
            partA.setPartitioned(true);
            partA.setPartitionSize(6);
            partA.setPartitionUnit(PartitionUnit.HOURS);
            partA.setPartitionTs(depATime);
            partA.setTargetId(tableA.getTargets()[0].getId());
            partA.setKey();
            var savePartA = partitionDAO.save(partA, false);
            assertTrue(savePartA.isSuccess());
        }

        // This check should show that for the first localdatetime, dependency A has been met
        TablePartitionResultSet res2 = tableDAO.processingPartitions(outTable, requestedTimes);
        TablePartitionResult[] res2failed = res2.getFailed();
        assertEquals(3, res2failed.length);
        assertEquals(1, res2failed[0].getResolvedDependencies().length);

        // Now add a snapshot-partition for table-b
        var newSnapshotPartB = new Partition();
        LocalDateTime newTime = LocalDateTime.now();
        newSnapshotPartB.setPartitioned(false);
        newSnapshotPartB.setPartitionTs(newTime);
        newSnapshotPartB.setTargetId(tableB.getTargets()[0].getId());
        newSnapshotPartB.setKey();
        var saveNewPartB = partitionDAO.save(newSnapshotPartB, false);
        assertTrue(saveNewPartB.isSuccess());

        // And the first partition should show success
        TablePartitionResultSet res3 = tableDAO.processingPartitions(outTable, requestedTimes);
        assertEquals(2, res3.getFailed().length);
        assertEquals(1, res3.getResolved().length);

        // Filling the outTable for the first partition
        var outPart = new Partition();
        outPart.setPartitioned(true);
        outPart.setPartitionSize(1);
        outPart.setPartitionUnit(PartitionUnit.DAYS);
        outPart.setPartitionTs(requestedTimes[0]);
        outPart.setTargetId(outTable.getTargets()[0].getId());
        outPart.setKey();
        var saveOutPart = partitionDAO.save(outPart, false);
        assertTrue(saveOutPart.isSuccess());

        // and the first partition should show processed
        TablePartitionResultSet res4 = tableDAO.processingPartitions(outTable, requestedTimes);
        assertEquals(2, res4.getFailed().length);
        assertEquals(1, res4.getProcessed().length);

        // fill the rest of dependency A save for 1
        LocalDateTime[] dependencyTimes5 = new LocalDateTime[] {
            LocalDateTime.of(2020, 2, 11, 0, 0),
            LocalDateTime.of(2020, 2, 11, 6, 0),
            LocalDateTime.of(2020, 2, 11, 12, 0),
            LocalDateTime.of(2020, 2, 11, 18, 0),
            LocalDateTime.of(2020, 2, 12, 0, 0),
            // Skip one here
            LocalDateTime.of(2020, 2, 12, 12, 0),
            LocalDateTime.of(2020, 2, 12, 18, 0),
        };
        for (LocalDateTime depATime: dependencyTimes5) {
            var partA = new Partition();
            partA.setPartitioned(true);
            partA.setPartitionSize(6);
            partA.setPartitionUnit(PartitionUnit.HOURS);
            partA.setPartitionTs(depATime);
            partA.setTargetId(tableA.getTargets()[0].getId());
            partA.setKey();
            var savePartA = partitionDAO.save(partA, false);
            assertTrue(savePartA.isSuccess());
        }
        TablePartitionResultSet res5 = tableDAO.processingPartitions(outTable, requestedTimes);
        assertEquals(1, res5.getResolved().length);
        assertEquals(1, res5.getFailed().length);
        assertEquals(1, res5.getProcessed().length);
        assertEquals(LocalDateTime.of(2020, 2, 12, 0, 0), res5.getFailedTs()[0]);
    }

    @Test
    void testPartitionlessTableDependency() throws SQLException {
        var tableBuilderA = new TableBuilder();
        Table tableA = tableBuilderA
            .area("test_dependency_area")
            .vertical("test_vertical")
            .table("tableA")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .source()
                .connectionName(singleSourceTestConnection.getName())
                .path("/further/disk/path/tableASource")
                .format("parquet")
                .version("testVersion")
                .select()
                    .query("SELECT * FROM <path>;")
                    .column("FIXME")    // TODO: Should not be necessary for unpartitioned tables
                    .done()
                .done()
            .target()
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        var saveA = tableDAO.save(tableA, false);
        assertTrue(saveA.isSuccess());

        var tableBuilderOut = new TableBuilder();
        Table outTable = tableBuilderOut
            .area("test_out_area")
            .vertical("test_vertical")
            .table("tableOut")
            .version("testVersion")
            .parallelism(1)
            .maxBulkSize(1)
            .dependency()
                .area(tableA.getArea())
                .vertical(tableA.getVertical())
                .table(tableA.getName())
                .version(tableA.getVersion())
                .format("parquet")
                .done()
            .target()
                .connectionName(singleDataLakeConnection.getName())
                .format("parquet")
                .done()
            .build();
        var saveOut = tableDAO.save(outTable, true);
        assertTrue(saveOut.isSuccess());

        // Test without having the dependency ready
        var requestTime = LocalDateTime.now();
        TablePartitionResultSet res1 = tableDAO.processingPartitions(outTable, new LocalDateTime[]{requestTime});
        assertEquals(1, res1.getFailed().length);

        // Add dependency partition
        var newSnapshotPartA = new Partition();
        newSnapshotPartA.setPartitioned(false);
        newSnapshotPartA.setPartitionTs(requestTime);
        newSnapshotPartA.setTargetId(tableA.getTargets()[0].getId());
        newSnapshotPartA.setKey();
        var saveNewPartB = partitionDAO.save(newSnapshotPartA, false);
        assertTrue(saveNewPartB.isSuccess());

        // Check again, it should be ready
        var requestTime2 = LocalDateTime.now();
        TablePartitionResultSet res2 = tableDAO.processingPartitions(outTable, new LocalDateTime[]{requestTime2});
        assertEquals(1, res2.getResolved().length);

        // Add a new output partition
        var newSnapshotOutPart = new Partition();
        newSnapshotOutPart.setPartitioned(false);
        newSnapshotOutPart.setPartitionTs(requestTime2);
        newSnapshotOutPart.setTargetId(outTable.getTargets()[0].getId());
        newSnapshotOutPart.setKey();
        var saveOutPart = partitionDAO.save(newSnapshotOutPart, false);
        assertTrue(saveOutPart.isSuccess());

        // Check for this new request time
        TablePartitionResultSet res3 = tableDAO.processingPartitions(outTable, new LocalDateTime[]{requestTime2});
        assertEquals(1, res3.getProcessed().length);
        // TODO: At the moment it will still say processed if requested for the same minute
    }

}
