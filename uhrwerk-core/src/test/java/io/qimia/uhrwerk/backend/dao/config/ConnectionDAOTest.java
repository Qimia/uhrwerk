package io.qimia.uhrwerk.backend.dao.config;

import io.qimia.uhrwerk.backend.model.config.Connection;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ConnectionDAOTest {

  java.sql.Connection db;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    db =
        DriverManager.getConnection(
            "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void save() throws SQLException {
    Connection conn = new Connection();
    conn.setConnectionName("Test-Conn1");
    conn.setConnectionUrl("Test-URl");
    conn.setConnectionType("Test-Type");
    conn.setVersion("1.0");
    Long id = ConnectionDAO.save(db, conn);
    assertNotNull(id);

    Connection dbConn = ConnectionDAO.get(db, id);
    assertNotNull(dbConn);
    assertEquals(id, dbConn.getId());
    assertEquals("Test-Conn1", dbConn.getConnectionName());
  }
}
