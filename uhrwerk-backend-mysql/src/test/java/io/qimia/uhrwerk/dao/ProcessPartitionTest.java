package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.*;
import io.qimia.uhrwerk.common.model.Connection;
import org.junit.jupiter.api.Test;
import org.scalatest.Ignore;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessPartitionTest {

    java.sql.Connection db;
    final String insertPartitionQuery = "INSERT INTO PARTITION_(id, target_id, partition_ts)\n"
            + "VALUES(?,?,?)";;

    Connection connFS;
    Table tableDepA;
    Target depATarget;
    LocalDateTime[] filledPartitionsA;
    Partition[] partitionsA;


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
        // WARNING deletes all data as cleanup
        var deletePartDependencies = db.createStatement();  // In case of some lost source data
        deletePartDependencies.execute("DELETE FROM PARTITION_DEPENDENCY");
        deletePartDependencies.close();
        var deletePartitions = db.createStatement();
        deletePartitions.execute("DELETE FROM PARTITION_");
        deletePartitions.close();
        var deleteDependencyStm = db.createStatement();
        deleteDependencyStm.execute("DELETE FROM DEPENDENCY");
        deleteDependencyStm.close();
        var deleteSourceStm = db.createStatement();  // In case of some lost source data
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

    public void setupTableA() throws SQLException {
        // Setup 2 tables to depend on with targets (which have a connection)
        connFS = new Connection();
        connFS.setName("a_connection");
        connFS.setType(ConnectionType.FS);
        connFS.setKey();
        Statement a = db.createStatement();
        a.executeUpdate("INSERT INTO CONNECTION (id, name, type, path) VALUES (" + connFS.getId() +
                ", 'a_connection', 'FS', '/some/test/path')");
        a.close();

        tableDepA = new Table();
        tableDepA.setArea("area1");
        tableDepA.setVertical("vertical1");
        tableDepA.setName("name1");
        tableDepA.setVersion("1.0");
        tableDepA.setPartitionSize(1);
        tableDepA.setPartitionUnit(PartitionUnit.HOURS);
        tableDepA.setKey();
        Statement b = db.createStatement();
        b.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + tableDepA.getId() + ", 'area1', 'vertical1', 'name1', '1.0', 'HOURS', 1, 1, 1)");
        b.close();

        depATarget = new Target();
        depATarget.setTableId(tableDepA.getId());
        depATarget.setFormat("parquet");
        depATarget.setKey();
        Statement c = db.createStatement();
        c.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
                "VALUES (" + depATarget.getId() + "," + tableDepA.getId() + "," + connFS.getId() + ", 'parquet')");
        c.close();

        filledPartitionsA = new LocalDateTime[]{
                LocalDateTime.of(2020, 4, 10, 10, 0),
                LocalDateTime.of(2020, 4, 10, 11, 0),
                LocalDateTime.of(2020, 4, 10, 12, 0)
        };
        partitionsA = new Partition[filledPartitionsA.length];
        PreparedStatement insert = db.prepareStatement(insertPartitionQuery);
        for (int i = 0; i < partitionsA.length; i++) {
            var p = new Partition();
            p.setTargetId(depATarget.getId());
            p.setPartitionTs(filledPartitionsA[i]);
            p.setPartitionUnit(PartitionUnit.HOURS);
            p.setPartitionSize(1);
            p.setKey();
            partitionsA[i] = p;


            insert.setLong(1, p.getId());
            insert.setLong(2, p.getTargetId());
            insert.setTimestamp(3, Timestamp.valueOf(p.getPartitionTs()));
            insert.addBatch();
        }
        insert.executeBatch();
        insert.close();
    }

    @Test
    void checkSingleIdentityDependency() throws SQLException {
        setupTableA();

        // Setup a simple one on one data model
        var tableOut = new Table();
        tableOut.setArea("area1");
        tableOut.setVertical("vertical1");
        tableOut.setName("tableout");
        tableOut.setVersion("1.0");
        tableOut.setPartitionSize(1);
        tableOut.setPartitionUnit(PartitionUnit.HOURS);
        tableOut.setKey();
        var dependencyIn = new Dependency();
        dependencyIn.setTableName(tableDepA.getName());
        dependencyIn.setArea(tableDepA.getArea());
        dependencyIn.setFormat(depATarget.getFormat());
        dependencyIn.setVertical(tableDepA.getVertical());
        dependencyIn.setVersion(tableDepA.getVersion());
        dependencyIn.setDependencyTableId(tableDepA.getId());
        dependencyIn.setDependencyTargetId(depATarget.getId());
        dependencyIn.setTableId(tableOut.getId());
        dependencyIn.setTransformType(PartitionTransformType.IDENTITY);
        dependencyIn.setKey();
        tableOut.setDependencies(new Dependency[]{dependencyIn});
        var targetOut = new Target();
        targetOut.setFormat("csv");
        targetOut.setTableId(tableOut.getId());
        targetOut.setConnection(connFS);
        targetOut.setKey();
        tableOut.setTargets(new Target[]{targetOut});

        Statement tableStm = db.createStatement();
        tableStm.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + tableOut.getId() + ", 'area1', 'vertical1', 'tableout', '1.0', 'HOURS', 1, 1, 1)");
        tableStm.close();

        PreparedStatement depStm = db.prepareStatement("INSERT INTO DEPENDENCY(id, table_id, dependency_target_id, dependency_table_id, " +
                "transform_type, transform_partition_size) VALUES (?, ?, ?, ?, 'IDENTITY', 1)");
        depStm.setLong(1, dependencyIn.getId());
        depStm.setLong(2, dependencyIn.getTableId());
        depStm.setLong(3, dependencyIn.getDependencyTargetId());
        depStm.setLong(4, dependencyIn.getDependencyTableId());
        depStm.executeUpdate();

        Statement tarStm = db.createStatement();
        tarStm.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
                "VALUES (" + targetOut.getId() + "," + tableOut.getId() + "," + connFS.getId() + ", 'csv')");
        tarStm.close();

        // Already write a single Partition for the output
        var outPartition = new Partition();
        outPartition.setTargetId(targetOut.getId());
        outPartition.setPartitionTs(filledPartitionsA[0]);     // The first one is already there
        outPartition.setPartitionUnit(PartitionUnit.HOURS);
        outPartition.setPartitionSize(1);
        outPartition.setKey();

        PreparedStatement partStm = db.prepareStatement(insertPartitionQuery);
        partStm.setLong(1, outPartition.getId());
        partStm.setLong(2, outPartition.getTargetId());
        partStm.setTimestamp(3, Timestamp.valueOf(outPartition.getPartitionTs()));
        partStm.executeUpdate();

        PreparedStatement partDepStm = db.prepareStatement("INSERT INTO PARTITION_DEPENDENCY (id, partition_id, dependency_partition_id)\n" +
                "VALUES (?, ?, ?);");
        long partDepId = PartitionDependencyHash.generateId(outPartition.getId(), partitionsA[0].getId());
        partDepStm.setLong(1, partDepId);
        partDepStm.setLong(2, outPartition.getId());
        partDepStm.setLong(3, partitionsA[0].getId());
        partDepStm.executeUpdate();

        // Now call processingPartitions
        var dao = new TableDAO(db);
        LocalDateTime[] testTimes = new LocalDateTime[]{
                filledPartitionsA[0],       // Should be already filled
                filledPartitionsA[1],       // ready to process
                filledPartitionsA[2],
                LocalDateTime.of(2020, 4, 10, 13, 0)    // Not ready yet
        };
        var resultSet = dao.processingPartitions(tableOut, testTimes);
        assertEquals(1, resultSet.getFailedTs().length);
        assertEquals(1, resultSet.getProcessedTs().length);
        assertEquals(2, resultSet.getResolvedTs().length);
    }

    @Test
    void checkSingleWindowedDependency() throws SQLException {
        setupTableA();

        // Setup a simple one on one data model
        var tableOut = new Table();
        tableOut.setArea("area1");
        tableOut.setVertical("vertical1");
        tableOut.setName("tableout");
        tableOut.setVersion("1.0");
        tableOut.setPartitionSize(1);
        tableOut.setPartitionUnit(PartitionUnit.HOURS);
        tableOut.setKey();
        var dependencyIn = new Dependency();
        dependencyIn.setTableName(tableDepA.getName());
        dependencyIn.setArea(tableDepA.getArea());
        dependencyIn.setFormat(depATarget.getFormat());
        dependencyIn.setVertical(tableDepA.getVertical());
        dependencyIn.setVersion(tableDepA.getVersion());
        dependencyIn.setDependencyTableId(tableDepA.getId());
        dependencyIn.setDependencyTargetId(depATarget.getId());
        dependencyIn.setTableId(tableOut.getId());
        dependencyIn.setTransformType(PartitionTransformType.WINDOW);
        dependencyIn.setTransformPartitionSize(2);
        dependencyIn.setKey();
        tableOut.setDependencies(new Dependency[]{dependencyIn});
        var targetOut = new Target();
        targetOut.setFormat("csv");
        targetOut.setTableId(tableOut.getId());
        targetOut.setConnection(connFS);
        targetOut.setKey();
        tableOut.setTargets(new Target[]{targetOut});

        Statement tableStm = db.createStatement();
        tableStm.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + tableOut.getId() + ", 'area1', 'vertical1', 'tableout', '1.0', 'HOURS', 1, 1, 1)");
        tableStm.close();

        PreparedStatement depStm = db.prepareStatement("INSERT INTO DEPENDENCY(id, table_id, dependency_target_id, dependency_table_id, " +
                "transform_type, transform_partition_size) VALUES (?, ?, ?, ?, 'WINDOW', 2)");
        depStm.setLong(1, dependencyIn.getId());
        depStm.setLong(2, dependencyIn.getTableId());
        depStm.setLong(3, dependencyIn.getDependencyTargetId());
        depStm.setLong(4, dependencyIn.getDependencyTableId());
        depStm.executeUpdate();

        Statement tarStm = db.createStatement();
        tarStm.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
                "VALUES (" + targetOut.getId() + "," + tableOut.getId() + "," + connFS.getId() + ", 'csv')");
        tarStm.close();

        // Already write a single Partition for the output
        var outPartition = new Partition();
        outPartition.setTargetId(targetOut.getId());
        outPartition.setPartitionTs(filledPartitionsA[1]);     // The second one is already there
        outPartition.setPartitionUnit(PartitionUnit.HOURS);
        outPartition.setPartitionSize(1);
        outPartition.setKey();

        PreparedStatement partStm = db.prepareStatement(insertPartitionQuery);
        partStm.setLong(1, outPartition.getId());
        partStm.setLong(2, outPartition.getTargetId());
        partStm.setTimestamp(3, Timestamp.valueOf(outPartition.getPartitionTs()));
        partStm.executeUpdate();

        // Now make sure that the 2nd partition depends on the 2 previous partitions
        PreparedStatement partDepStm = db.prepareStatement("INSERT INTO PARTITION_DEPENDENCY (id, partition_id, dependency_partition_id)\n" +
                "VALUES (?, ?, ?);");
        long partDepId = PartitionDependencyHash.generateId(outPartition.getId(), partitionsA[0].getId());
        partDepStm.setLong(1, partDepId);
        partDepStm.setLong(2, outPartition.getId());
        partDepStm.setLong(3, partitionsA[0].getId());
        partDepStm.executeUpdate();
        partDepId = PartitionDependencyHash.generateId(outPartition.getId(), partitionsA[1].getId());
        partDepStm.setLong(1, partDepId);
        partDepStm.setLong(2, outPartition.getId());
        partDepStm.setLong(3, partitionsA[1].getId());
        partDepStm.executeUpdate();

        // Now call processingPartitions
        var dao = new TableDAO(db);
        LocalDateTime[] testTimes = new LocalDateTime[]{
                filledPartitionsA[0],       // Can't run because 9 o clock isn't there
                filledPartitionsA[1],       // has already been filled
                filledPartitionsA[2],       // is ready to run
                LocalDateTime.of(2020, 4, 10, 13, 0)    // Can't run because 13 isnt there
        };
        var resultSet = dao.processingPartitions(tableOut, testTimes);
        assertEquals(2, resultSet.getFailedTs().length);
        assertEquals(testTimes[1], resultSet.getProcessedTs()[0]);
        assertEquals(testTimes[2], resultSet.getResolvedTs()[0]);
    }

    @Test
    void checkSingleAggregatedDependency() throws SQLException {
        setupTableA();

        // Setup a simple one on one data model
        var tableOut = new Table();
        tableOut.setArea("area1");
        tableOut.setVertical("vertical1");
        tableOut.setName("tableout");
        tableOut.setVersion("1.0");
        tableOut.setPartitionSize(2);
        tableOut.setPartitionUnit(PartitionUnit.HOURS);
        tableOut.setKey();
        var dependencyIn = new Dependency();
        dependencyIn.setTableName(tableDepA.getName());
        dependencyIn.setArea(tableDepA.getArea());
        dependencyIn.setFormat(depATarget.getFormat());
        dependencyIn.setVertical(tableDepA.getVertical());
        dependencyIn.setVersion(tableDepA.getVersion());
        dependencyIn.setDependencyTableId(tableDepA.getId());
        dependencyIn.setDependencyTargetId(depATarget.getId());
        dependencyIn.setTableId(tableOut.getId());
        dependencyIn.setTransformType(PartitionTransformType.AGGREGATE);
        dependencyIn.setTransformPartitionSize(2);
        dependencyIn.setKey();
        tableOut.setDependencies(new Dependency[]{dependencyIn});
        var targetOut = new Target();
        targetOut.setFormat("csv");
        targetOut.setTableId(tableOut.getId());
        targetOut.setConnection(connFS);
        targetOut.setKey();
        tableOut.setTargets(new Target[]{targetOut});

        Statement tableStm = db.createStatement();
        tableStm.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + tableOut.getId() + ", 'area1', 'vertical1', 'tableout', '1.0', 'HOURS', 2, 1, 1)");
        tableStm.close();

        PreparedStatement depStm = db.prepareStatement("INSERT INTO DEPENDENCY(id, table_id, dependency_target_id, dependency_table_id, " +
                "transform_type, transform_partition_size) VALUES (?, ?, ?, ?, 'AGGREGATE', 2)");
        depStm.setLong(1, dependencyIn.getId());
        depStm.setLong(2, dependencyIn.getTableId());
        depStm.setLong(3, dependencyIn.getDependencyTargetId());
        depStm.setLong(4, dependencyIn.getDependencyTableId());
        depStm.executeUpdate();

        Statement tarStm = db.createStatement();
        tarStm.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
                "VALUES (" + targetOut.getId() + "," + tableOut.getId() + "," + connFS.getId() + ", 'csv')");
        tarStm.close();

        // First test without having any partitions written (with 2 hour batches ofcourse)
        var dao = new TableDAO(db);
        LocalDateTime[] testTimes = new LocalDateTime[]{
                filledPartitionsA[0],       // should be ready to run
                filledPartitionsA[2],       // Should be missing the 13 hour batch
        };
        var resultSet = dao.processingPartitions(tableOut, testTimes);
        assertEquals(testTimes[1], resultSet.getFailedTs()[0]);
        assertEquals(0, resultSet.getProcessedTs().length);
        assertEquals(testTimes[0], resultSet.getResolvedTs()[0]);

        // Now we fill the first testTimes partition for the outtable
        var outPartition = new Partition();
        outPartition.setTargetId(targetOut.getId());
        outPartition.setPartitionTs(filledPartitionsA[0]);     // The second one is already there
        outPartition.setPartitionUnit(PartitionUnit.HOURS);
        outPartition.setPartitionSize(2);
        outPartition.setKey();

        PreparedStatement partStm = db.prepareStatement(insertPartitionQuery);
        partStm.setLong(1, outPartition.getId());
        partStm.setLong(2, outPartition.getTargetId());
        partStm.setTimestamp(3, Timestamp.valueOf(outPartition.getPartitionTs()));
        partStm.executeUpdate();

        // Now make sure that the 1st partition depends on the 2 partitions of the dependency
        PreparedStatement partDepStm = db.prepareStatement("INSERT INTO PARTITION_DEPENDENCY (id, partition_id, dependency_partition_id)\n" +
                "VALUES (?, ?, ?);");
        long partDepId = PartitionDependencyHash.generateId(outPartition.getId(), partitionsA[0].getId());
        partDepStm.setLong(1, partDepId);
        partDepStm.setLong(2, outPartition.getId());
        partDepStm.setLong(3, partitionsA[0].getId());
        partDepStm.executeUpdate();
        partDepId = PartitionDependencyHash.generateId(outPartition.getId(), partitionsA[1].getId());
        partDepStm.setLong(1, partDepId);
        partDepStm.setLong(2, outPartition.getId());
        partDepStm.setLong(3, partitionsA[1].getId());
        partDepStm.executeUpdate();

        // Now we do the same processingPartitions call as before and we should see a processedTs
        resultSet = dao.processingPartitions(tableOut, testTimes);
        assertEquals(testTimes[1], resultSet.getFailedTs()[0]);
        assertEquals(testTimes[0], resultSet.getProcessedTs()[0]);
        assertEquals(0, resultSet.getResolvedTs().length);
    }


}
