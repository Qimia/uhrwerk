package io.qimia.uhrwerk.models.dao;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DataConfigTableDAOTest {

  Connection db;
  DtTableDAO tableDao;
  ConfigDAO connDao;
  DagTableSpecDAO tableSpecDao;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    db =
        DriverManager.getConnection(
            "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
    tableDao = new DtTableDAO(db);
    connDao = new ConfigDAO(db);
    tableSpecDao = new DagTableSpecDAO(db);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void save() throws SQLException {
    db.setAutoCommit(false);
    DagConnection conn =
        new DagConnection("Test-Conn1", "Test-Type", "Test-URl", "1.0", "Unit Test Connection");
    Long connId = connDao.saveConnection(conn);
    DagTableSpec tableSpec =
        new DagTableSpec(
            "Test-Area", "Test-Vertical", "Test-Table1", "1.0", false, "Unit Test Connection");
    Long tableSpecId = tableSpecDao.save(tableSpec);
    DtTable table =
        new DtTable(tableSpecId, connId, "Test-Path", "1.0", false, "Unit Test Connection");
    tableDao.save(table);
    db.commit();
  }

  @Test
  void transaction() throws Exception {
    db.setAutoCommit(false);
    DagConnection conn =
        new DagConnection("Test-Conn1", "Test-Type", "Test-URl", "1.0", "Unit Test Connection");
    Long connId = connDao.saveConnection(conn);
    DagTableSpec tableSpec =
        new DagTableSpec(
            "Test-Area", "Test-Vertical", "Test-Table1", "1.0", false, "Unit Test Connection");
    Long tableSpecId = tableSpecDao.save(tableSpec);
    DtTable table =
        new DtTable(tableSpecId, connId, "Test-Path", "1.0", false, "Unit Test Connection");
    tableDao.save(table);
    throw new Exception("some exception!");
  }
}
