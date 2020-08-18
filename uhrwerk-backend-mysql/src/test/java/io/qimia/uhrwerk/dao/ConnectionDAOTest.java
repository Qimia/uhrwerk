package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.ConnectionResult;
import io.qimia.uhrwerk.common.metastore.config.ConnectionService;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.ConnectionType;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionDAOTest {

  java.sql.Connection db;
  ConnectionService service;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    db =
        DriverManager.getConnection(
            "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
    service = new ConnectionDAO(db);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void insert() {
    Connection conn = new Connection();
    conn.setName("Test-Conn1");
    conn.setType(ConnectionType.FS);
    conn.setPath("/some/path/test1");
    conn.setKey();
    ConnectionResult result = service.save(conn, true);
    assertTrue(result.isSuccess());
    assertNotNull(result.getNewConnection());
    assertNotNull(result.getNewConnection().getId());
    System.out.println(result.getNewConnection());
  }

  @Test
  void update() {
    Connection conn = new Connection();
    conn.setName("Test-Conn1");
    conn.setType(ConnectionType.S3);
    conn.setPath("/some/path/updated");
    conn.setAwsAccessKeyID("access-key-id1");
    conn.setAwsSecretAccessKey("secret-access-key1");
    conn.setKey();
    ConnectionResult result = service.save(conn, true);
    assertTrue(result.isSuccess());
    assertNotNull(result.getNewConnection());
    assertNotNull(result.getNewConnection().getId());
    System.out.println(result.getNewConnection());
  }
}
