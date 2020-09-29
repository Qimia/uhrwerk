package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.ConnectionHelper;
import io.qimia.uhrwerk.common.metastore.config.PartitionResult;
import io.qimia.uhrwerk.common.model.*;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class PartitionDAOTest {
    java.sql.Connection db;
    private final Logger logger = Logger.getLogger(this.getClass());

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
        table.setClassName(
                String.join(
                        ".", table.getArea(), table.getVertical(), table.getName(), table.getVersion()));
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
        partition.setPartitioned(false);
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
        db = ConnectionHelper.getConnection();
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

        logger.info(partitionResult.getMessage());
        assertTrue(partitionResult.isSuccess());
        assertFalse(partitionResult.isError());
        assertNotNull(partitionResult.getNewResult());

        partition.setPartitionSize(234568);

        // this should succeed
        partitionResult = partitionDAO.save(partition, true);
        logger.info(partitionResult.getMessage());
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

        int oldSize = partition.getPartitionSize();
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
        new TableDAO(db).save(table, true); // target should be saved here

        partitionResult = partitionDAO.save(partition, false);
        assertFalse(partitionResult.isError());
        assertFalse(partitionResult.isSuccess());
        assertNotNull(partitionResult.getNewResult());
        logger.info(partitionResult.getMessage());
        assertTrue(partitionResult.getMessage().contains("exists in the Metastore"));

        // if the partition doesn't change it will succeed

        partition.setPartitionSize(oldSize);
        partitionResult = partitionDAO.save(partition, false);

        assertTrue(partitionResult.isSuccess());
        assertFalse(partitionResult.isError());
        assertNotNull(partitionResult.getNewResult());
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
        new TableDAO(db).save(table, true);
        // save target
        new TargetDAO(db).save(new Target[]{target}, table.getId(), false);

        // prepare some partitions
        Partition[] partitions = new Partition[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            Partition p = generatePartition(testName);
            p.setPartitionTs(timestamps[i]);
            p.setKey();
            partitions[i] = p;
//            PartitionResult partitionResult = partitionDAO.save(p, false);
        }
        PartitionResult[] partitionResults = partitionDAO.save(partitions, false);

        for (PartitionResult partitionResult : partitionResults) {
            if (partitionResult.getMessage() != null) {
                logger.info(partitionResult.getMessage());
            }
            assertTrue(partitionResult.isSuccess());
        }

        Partition[] partitionsSubset = Arrays.copyOfRange(partitions, 2, 4);

        Partition expectedLatestPartition = partitions[partitions.length - 1];

        Partition[] foundPartitions = partitionDAO.getPartitions(target.getId(), timestamps);

        assertEquals(timestamps.length, foundPartitions.length);
        logger.info(Arrays.toString(foundPartitions));
        logger.info(Arrays.toString(partitions));
        Arrays.sort(foundPartitions);
        Arrays.sort(partitions);
        assertTrue(Arrays.equals(foundPartitions, partitions));

        // getting only a subset of the timestamps
        LocalDateTime[] timestampsSubset = Arrays.copyOfRange(timestamps, 2, 4);
        Partition[] foundPartitionsSubset = partitionDAO.getPartitions(target.getId(), timestampsSubset);

        assertEquals(partitionsSubset.length, foundPartitionsSubset.length);
        Arrays.sort(foundPartitionsSubset);
        Arrays.sort(partitionsSubset);
        logger.info(Arrays.toString(foundPartitionsSubset));
        logger.info(Arrays.toString(partitionsSubset));
        assertTrue(Arrays.equals(foundPartitionsSubset, partitionsSubset));

        // getting the latest partition
        Partition latestPartition = partitionDAO.getLatestPartition(target.getId());
        assertNotNull(latestPartition);
        assertEquals(expectedLatestPartition, latestPartition);

        // missing target
        assertEquals(0, partitionDAO.getPartitions(1234L, timestamps).length);

        // missing table
        deleteTable(testName);
        assertEquals(0, partitionDAO.getPartitions(target.getId(), timestamps).length);
    }
}
