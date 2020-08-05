package io.qimia.uhrwerk.models.dao;

import io.qimia.uhrwerk.models.db.DagConnection;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class DagConnectionDAOTest {

  Connection db;
  DagConnectionDAO dao;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws ClassNotFoundException, SQLException {
    db =
        DriverManager.getConnection(
            "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
    dao = new DagConnectionDAO(db);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void save() throws SQLException {
    DagConnection conn =
        new DagConnection("Test-Conn1", "Test-Type", "Test-URl", "1.0", "Unit Test Connection");
    dao.save(conn);
  }
}
