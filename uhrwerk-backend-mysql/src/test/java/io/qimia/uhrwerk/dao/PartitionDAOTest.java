package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.PartitionResult;
import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionDAOTest {
    java.sql.Connection db;

    private final LocalDateTime[] timestamps = {LocalDateTime.of(2020, 8, 20, 15, 0),
            LocalDateTime.of(2020, 8, 20, 16, 0),
            LocalDateTime.of(2020, 8, 20, 17, 0),
            LocalDateTime.of(2020, 8, 20, 18, 0),
            LocalDateTime.of(2020, 8, 20, 19, 0),
            LocalDateTime.of(2020, 8, 22, 19, 0)};

    private static Table generateTable(String testName) {
        Table table = new Table();
        table.setArea("test-area-partition");
        table.setVertical("test-vertical");
        table.setName(testName);
        table.setPartitionUnit(PartitionUnit.MINUTES);
        table.setPartitionSize(15);
        table.setParallelism(8);
        table.setMaxBulkSize(96);
        table.setVersion("1.0");
        table.setKey();
        table.setTargets(new Target[]{generateTarget(testName, table.getId())});

        return table;
    }

    private static Connection generateConnection(String testName) {
        Connection connection = new Connection();
        connection.setName(testName);
        connection.setType(ConnectionType.FS);
        connection.setPath("/some/path/test1");
        connection.setKey();

        return connection;
    }

    private static Target generateTarget(String testName, Long tableId) {
        Target target = new Target();
        target.setFormat("csv");
        target.setConnection(generateConnection(testName));
        target.setTableId(tableId);
        target.setKey();

        return target;
    }

    private Partition generatePartition(String testName) {
        Table table = generateTable(testName);

        Partition partition = new Partition();

        partition.setTargetId(generateTarget(testName, table.getId()).getId());
        partition.setPartitionTs(timestamps[5]);
        partition.setPartitionUnit(table.getPartitionUnit());
        partition.setPartitionSize(table.getPartitionSize());
        partition.setKey();

        return partition;
    }

    private void deleteTable(String testName) {
        try {
            db.prepareStatement("delete from TABLE_ where id = " + generateTable(testName).getId()).execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws SQLException {
        db = DriverManager.getConnection(
                "jdbc:mysql://localhost:53306/UHRWERK_METASTORE",
                "UHRWERK_USER",
                "Xq92vFqEKF7TB8H9"
        );
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws SQLException {
        db.prepareStatement("delete from CONNECTION").execute();
        db.prepareStatement("delete from TARGET").execute();
        db.prepareStatement("delete from TABLE_").execute();
        db.prepareStatement("delete from PARTITION_").execute();

        if (db != null && !db.isClosed()) {
            db.close();
        }
    }

    @Test
    void savePartitionShouldSaveIt() {
        PartitionDAO partitionDAO = new PartitionDAO(db);

        Partition partition = generatePartition("savePartitionShouldSaveIt");

        PartitionResult partitionResult = partitionDAO.save(partition, false);

        assertTrue(partitionResult.isSuccess());
        assertFalse(partitionResult.isError());
        assertNotNull(partitionResult.getNewResult());
    }

    @Test
    void saveChangedPartitionShouldSaveItWithOverwrite() {
        PartitionDAO partitionDAO = new PartitionDAO(db);
        String testName = "saveChangedPartitionShouldSaveItWithOverwrite";

        Partition partition = generatePartition(testName);

        PartitionResult partitionResult = partitionDAO.save(partition, false);

        System.out.println(partitionResult.getMessage());
        assertTrue(partitionResult.isSuccess());
        assertFalse(partitionResult.isError());
        assertNotNull(partitionResult.getNewResult());

        partition.setPartitionSize(234568);

        // this should succeed
        partitionResult = partitionDAO.save(partition, true);
        System.out.println(partitionResult.getMessage());
        assertTrue(partitionResult.isSuccess());
        assertFalse(partitionResult.isError());
    }

    @Test
    void saveChangedPartitionShouldFailWithoutOverwrite() throws SQLException {
        PartitionDAO partitionDAO = new PartitionDAO(db);
        String testName = "saveChangedPartitionShouldFailWithoutOverwrite";

        Partition partition = generatePartition(testName);

        PartitionResult partitionResult = partitionDAO.save(partition, false);

        assertTrue(partitionResult.isSuccess());
        assertFalse(partitionResult.isError());
        assertNotNull(partitionResult.getNewResult());

        partition.setPartitionSize(234568);
        partitionResult = partitionDAO.save(partition, false);

        // this will fail because no partition is found as its table and target are missing (the join fails)
        assertTrue(partitionResult.isError());
        assertFalse(partitionResult.isSuccess());
        assertNotNull(partitionResult.getNewResult());

        // saving the table and target
        // this fails as the partition is different than the one in the metastore
        Table table = generateTable(testName);
        Connection connection = generateConnection(testName);

        // save connection
        new ConnectionDAO(db).save(connection, true);
        // save table
        new TableDAO(db).save(table); // target should be saved here

        partitionResult = partitionDAO.save(partition, false);
        assertTrue(partitionResult.isError());
        assertFalse(partitionResult.isSuccess());
        assertNotNull(partitionResult.getNewResult());
        System.out.println(partitionResult.getMessage());
        assertTrue(partitionResult.getMessage().contains("exists in the Metastore"));
    }

    @Test
    void getPartitionsShouldGetPartitionsByTargetIdAndTimestamps() throws SQLException {
        PartitionDAO partitionDAO = new PartitionDAO(db);
        String testName = "getPartitionsShouldGetPartitionsByTargetIdAndTimestamps";

        Table table = generateTable(testName);
        Target target = generateTarget(testName, table.getId());
        Connection connection = generateConnection(testName);

        // save connection
        new ConnectionDAO(db).save(connection, true);
        // save table
        new TableDAO(db).saveTable(table);
        // save target
        new TargetDAO(db).save(new Target[]{target}, table.getId(), false);

        // prepare some partitions
        Partition[] partitions = new Partition[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            Partition p = generatePartition(testName);
            p.setPartitionTs(timestamps[i]);
            p.setKey();
            partitions[i] = p;
            PartitionResult partitionResult = partitionDAO.save(p, false);

            if (partitionResult.getMessage() != null) {
                System.out.println(partitionResult.getMessage());
            }
            assertTrue(partitionResult.isSuccess());
        }
        Partition[] partitionsSubset = Arrays.copyOfRange(partitions, 2, 4);

        Partition[] foundPartitions = partitionDAO.getPartitions(target.getId(), timestamps);

        assertEquals(timestamps.length, foundPartitions.length);
        System.out.println(Arrays.toString(foundPartitions));
        System.out.println(Arrays.toString(partitions));
        Arrays.sort(foundPartitions);
        Arrays.sort(partitions);
        assertTrue(Arrays.equals(foundPartitions, partitions));

        // getting only a subset of the timestamps
        LocalDateTime[] timestampsSubset = Arrays.copyOfRange(timestamps, 2, 4);
        Partition[] foundPartitionsSubset = partitionDAO.getPartitions(target.getId(), timestampsSubset);

        assertEquals(partitionsSubset.length, foundPartitionsSubset.length);
        Arrays.sort(foundPartitionsSubset);
        Arrays.sort(partitionsSubset);
        System.out.println(Arrays.toString(foundPartitionsSubset));
        System.out.println(Arrays.toString(partitionsSubset));
        assertTrue(Arrays.equals(foundPartitionsSubset, partitionsSubset));

        // missing target
        assertEquals(0, partitionDAO.getPartitions(1234L, timestamps).length);

        // missing table
        deleteTable(testName);
        assertEquals(0, partitionDAO.getPartitions(target.getId(), timestamps).length);
    }
}
