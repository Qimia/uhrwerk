package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.PartitionResult;
import io.qimia.uhrwerk.common.metastore.config.SourceService;
import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitionDAOTest {
    java.sql.Connection db;
    SourceService service;

    private final LocalDateTime[] timestamps = {LocalDateTime.of(2020, 8, 20, 15, 0),
            LocalDateTime.of(2020, 8, 20, 16, 0),
            LocalDateTime.of(2020, 8, 20, 17, 0),
            LocalDateTime.of(2020, 8, 20, 18, 0),
            LocalDateTime.of(2020, 8, 20, 19, 0)};

    private Table generateTable() {
        Table table = new Table();
        table.setArea("test-area-source");
        table.setVertical("test-vertical");
        table.setName("test-table-sourcedao");
        table.setPartitionUnit(PartitionUnit.MINUTES);
        table.setPartitionSize(15);
        table.setParallelism(8);
        table.setMaxBulkSize(96);
        table.setVersion("1.0");
        table.setKey();

        return table;
    }

    Connection generateConnection() {
        Connection connection = new Connection();
        connection.setName("Test-Conn-Source");
        connection.setType(ConnectionType.FS);
        connection.setPath("/some/path/test1");
        connection.setKey();

        return connection;
    }

    Target generateTarget() {
        Target target = new Target();
        target.setFormat("csv");
        target.setConnection(generateConnection());
        target.setTableId(generateTable().getId());
        target.setKey();

        return target;
    }

    Partition generatePartition() {
        Table table = generateTable();

        Partition partition = new Partition();

        partition.setTargetId(generateTarget().getId());
        partition.setPartitionTs(timestamps[0]);
        partition.setPartitionUnit(table.getPartitionUnit());
        partition.setPartitionSize(table.getPartitionSize());
        partition.setKey();

        return partition;
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() throws SQLException {
        db =
                DriverManager.getConnection(
                        "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
        service = new SourceDAO(db);

        // clean up
        db.prepareStatement("delete from PARTITION_").execute();
        db.prepareStatement("delete from CONNECTION where id = " + generateConnection().getId()).execute();
        db.prepareStatement("delete from TABLE_ where id = " + generateTable().getId()).execute();
        db.prepareStatement("delete from TARGET where id = " + generateTarget().getId()).execute();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws SQLException {
        if (db != null) if (!db.isClosed()) db.close();
    }

    @Test
    void getPartitionsShouldGetPartitionsByTargetIdAndTimestamps() throws SQLException {
        PartitionDAO partitionDAO = new PartitionDAO(db);

        Table table = generateTable();
        Target target = generateTarget();
        Connection connection = generateConnection();

        // save connection
        new ConnectionDAO(db).save(connection, true);
        // save table
        new TableDAO(db).saveTable(table);
        // save target
        new TargetDAO(db).save(new Target[]{target}, table.getId(), false);

        // prepare some partitions
        Partition[] partitions = new Partition[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            Partition p = generatePartition();
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
    }
}
