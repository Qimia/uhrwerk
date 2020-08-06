package io.qimia.uhrwerk.models.dao;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DagConfigTableSpecDAOTest {

  Connection db;
  DagTableSpecDAO dao;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    db =
        DriverManager.getConnection(
            "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
    dao = new DagTableSpecDAO(db);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void save() throws SQLException {
    DagTableSpec tableSpec =
        new DagTableSpec(
            "Test-Area", "Test-Vertical", "Test-Table1", "1.0", false, "Unit Test Connection");
    dao.save(tableSpec);
  }
}
