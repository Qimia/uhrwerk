package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.Dependency;
import io.qimia.uhrwerk.common.model.Table;
import io.qimia.uhrwerk.common.model.Target;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class DependencyDAOTest {

    java.sql.Connection db;

//    @org.junit.jupiter.api.BeforeEach
//    void setUp() throws SQLException {
//        db = DriverManager.getConnection(
//                "jdbc:mysql://localhost:53306/UHRWERK_METASTORE",
//                "UHRWERK_USER",
//                "Xq92vFqEKF7TB8H9"
//        );
//
//        var conn = new Connection();
//        conn.setName("a_connection");
//        conn.setKey();
//        Statement a = db.createStatement();
//        a.executeUpdate("INSERT INTO CONNECTION (id, name, type, path) VALUES (" + conn.getId() +
//                ", 'a_connection', 'fs', '/some/test/path')");
//        a.close();
//
//        var tableDepA = new Table();
//        tableDepA.setArea("area1");
//        tableDepA.setVertical("vertical1");
//        tableDepA.setName("name1");
//        tableDepA.setVersion("1.0");
//        tableDepA.setKey();
//        Statement b = db.createStatement();
//        b.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
//                "VALUES (" + tableDepA.getId() + ", 'area1', 'vertical1', 'name1', '1.0', 'HOURS', 1, 1, 1)");
//        b.close();
//
//        var depATarget = new Target();
//        depATarget.setTableId(tableDepA.getId());
//        depATarget.setFormat("parquet");
//        depATarget.setKey();
//        Statement c = db.createStatement();
//        c.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
//                "VALUES (" + depATarget.getId() + "," + tableDepA.getId() + "," + conn.getId() + ", 'parquet')");
//        c.close();
//
//        var tableDepB = new Table();
//        tableDepB.setArea("area1");
//        tableDepB.setVertical("vertical1");
//        tableDepB.setName("name2");
//        tableDepB.setVersion("1.0");
//        tableDepB.setKey();
//        Statement d = db.createStatement();
//        d.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
//                "VALUES (" + tableDepB.getId() + ", 'area1', 'vertical1', 'name2', '1.0', 'HOURS', 1, 1, 1)");
//        d.close();
//
//        var depBTarget = new Target();
//        depBTarget.setTableId(tableDepB.getId());
//        depBTarget.setFormat("parquet");
//        depBTarget.setKey();
//        Statement e = db.createStatement();
//        e.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
//                "VALUES (" + depBTarget.getId() + "," + tableDepB.getId() + "," + conn.getId() + ", 'parquet')");
//        e.close();
//    }
//
//    @org.junit.jupiter.api.AfterEach
//    void tearDown() throws SQLException {
//        if (db != null) if (!db.isClosed()) db.close();
//    }

    @Test
    void findTest() throws SQLException {
//        var newTable = new Table();
//        newTable.setArea("area1");
//        newTable.setVertical("vertical1");
//        newTable.setName("name3");
//        newTable.setVersion("1.0");
//        newTable.setKey();
//        Statement setupTableStm = db.createStatement();
//        setupTableStm.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
//                "VALUES (" + newTable.getId() + ", 'area1', 'vertical1', 'name3', '1.0', 'HOURS', 1, 1, 1)");
//        setupTableStm.close();
//
//        var depA = new Dependency();
//        depA.setArea("area1");
//        depA.setVertical("vertical1");
//        depA.setTableName("name1");
//        depA.setVersion("1.0");
//        depA.setFormat("parquet");
//        depA.setKey();
//        var depB = new Dependency();
//        depB.setArea("area1");
//        depB.setVertical("vertical1");
//        depB.setTableName("name2");
//        depB.setVersion("1.0");
//        depB.setFormat("parquet");
//        depB.setKey();
//        newTable.setDependencies(new Dependency[]{depA, depB});
//
//        DependencyDAO dao = new DependencyDAO(db);
//        DependencyDAO.FindQueryResult checkRes = dao.findTables(newTable.getDependencies());
//        System.out.println("HeyHey");
    }

    @Test
    void checkKeyGeneration() {
        var tableDepA = new Table();
        tableDepA.setArea("area1");
        tableDepA.setVertical("vertical1");
        tableDepA.setName("name1");
        tableDepA.setVersion("1.0");
        tableDepA.setKey();
        var depATarget = new Target();
        depATarget.setTableId(tableDepA.getId());
        depATarget.setFormat("parquet");
        depATarget.setKey();

        var tableB = new Table();
        tableB.setArea("area1");
        tableB.setVertical("vertical1");
        tableB.setName("name2");
        tableB.setVersion("1.0");
        tableB.setKey();
        var depA = new Dependency();
        depA.setArea("area1");
        depA.setVertical("vertical1");
        depA.setTableName("name1");
        depA.setVersion("1.0");
        depA.setFormat("parquet");
        depA.setTableId(tableB.getId());
        depA.setKey();
        assertEquals(depATarget.getId(), depA.getTargetId());
    }
}
