package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.TargetResult;
import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class TargetDAOTest {

    java.sql.Connection db;

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

    Connection setupFSConnection() throws SQLException {
        Connection conn = new Connection();
        conn.setName("fsconn");
        conn.setPath("/aa/bb/cc/");
        conn.setType(ConnectionType.FS);
        conn.setKey();
        Statement setupTableStm = db.createStatement();
        setupTableStm.executeUpdate("INSERT INTO CONNECTION(id, name, type, path)" +
                "VALUES (" + conn.getId() + ", 'fsconn', 'FS', '/aa/bb/cc/')");
        setupTableStm.close();
        return conn;
    }

    Connection setupJDBCConnection() throws SQLException {
        Connection conn = new Connection();
        conn.setName("jdbcconn");
        conn.setJdbcPass("pass");
        conn.setJdbcUser("root");
        conn.setJdbcUrl("somejdbcurl");
        conn.setJdbcDriver("somejdbcdriver");
        conn.setType(ConnectionType.JDBC);
        conn.setKey();
        Statement setupTableStm = db.createStatement();
        setupTableStm.executeUpdate("INSERT INTO CONNECTION(id, name, type, jdbc_pass, jdbc_user, jdbc_url, " +
                "jdbc_driver) VALUES (" + conn.getId() + ", 'jdbcconn', 'JDBC', 'pass', 'root', 'somejdbcurl', " +
                "'somejdbcdriver')");
        setupTableStm.close();
        return conn;
    }

    Table setupTable() throws SQLException {
        var newTable = new Table();
        newTable.setArea("area1");
        newTable.setVertical("vertical1");
        newTable.setName("tablename");
        newTable.setVersion("1.0");
        newTable.setPartitionUnit(PartitionUnit.HOURS);
        newTable.setPartitionSize(1);
        newTable.setKey();
        Statement setupTableStm = db.createStatement();
        setupTableStm.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + newTable.getId() + ", 'area1', 'vertical1', 'tablename', '1.0', 'HOURS', 1, 1, 1)");
        setupTableStm.close();
        return newTable;
    }

    @Test
    void writeReadDelete() throws SQLException {
        Table t = setupTable();
        Connection fsConn = setupFSConnection();
        Target tarA = new Target();
        tarA.setFormat("parquet");
        tarA.setConnection(fsConn);
        tarA.setTableId(t.getId());
        tarA.setKey();

        Connection dbConn = setupJDBCConnection();
        Target tarB = new Target();
        tarB.setFormat("jdbc");
        tarB.setConnection(dbConn);
        tarB.setTableId(t.getId());
        tarB.setKey();

        TargetDAO dao = new TargetDAO(db);
        // write
        dao.insertTargets(new Target[]{tarA, tarB});

        // read
        Target[] retrievedTargets = dao.getTableTargets(t.getId());
        assertEquals(2, retrievedTargets.length);
        for (Target targetFound : retrievedTargets) {
            assertTrue((targetFound.getFormat().equals("parquet")) || (targetFound.getFormat().equals("jdbc")));
        }

        // delete
        dao.deleteTargetsByTable(t.getId());

        // read again
        retrievedTargets = dao.getTableTargets(t.getId());
        assertEquals(0, retrievedTargets.length);
    }

    @Test
    void fullSaveTest() throws SQLException {
        Table t = setupTable();
        Connection fsConn = setupFSConnection();
        Target tarA = new Target();
        tarA.setFormat("parquet");
        tarA.setConnection(fsConn);
        tarA.setTableId(t.getId());
        tarA.setKey();

        Connection dbConn = setupJDBCConnection();
        Target tarB = new Target();
        tarB.setFormat("jdbc");
        tarB.setConnection(dbConn);
        tarB.setTableId(t.getId());
        tarB.setKey();
        Target[] targets = {tarA, tarB};

        TargetDAO dao = new TargetDAO(db);
        TargetResult res1 = dao.save(targets, t.getId(), true);
        assertTrue(res1.isSuccess());

        dao.deleteTargetsByTable(t.getId());
        TargetResult res2 = dao.save(targets, t.getId(), false);
        assertTrue(res2.isSuccess());
        TargetResult res3 = dao.save(targets, t.getId(), false);
        assertTrue(res3.isSuccess());

        tarB.setFormat("badformatchange");
        TargetResult res4 = dao.save(targets, t.getId(), false);
        assertFalse(res4.isSuccess());
    }


    @Test
    void compareTest() {
        var connA = new Connection();
        connA.setName("connA");
        connA.setKey();
        var tarA = new Target();
        tarA.setFormat("jdbc");
        tarA.setTableId(123L);
        tarA.setConnection(connA);
        tarA.setKey();

        var connAFull = new Connection();
        connAFull.setName("connA");
        connAFull.setJdbcDriver("somedriver");
        connAFull.setJdbcUrl("someurl");
        connAFull.setJdbcUser("root");
        connAFull.setJdbcPass("somePass");
        var tarB = new Target();
        tarB.setFormat("jdbc");
        tarB.setTableId(123L);
        tarB.setConnection(connAFull);
        assertTrue(TargetDAO.compareTargets(tarB, tarA));

        tarA.setFormat("parquet");
        tarA.setKey();
        assertFalse(TargetDAO.compareTargets(tarB, tarA));

        tarA.setFormat("jdbc");
        connA.setName("newname");
        connA.setKey();
        tarA.setKey();
        assertFalse(TargetDAO.compareTargets(tarB, tarA));
    }
}
