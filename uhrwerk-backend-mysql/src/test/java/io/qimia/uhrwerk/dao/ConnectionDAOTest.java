package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.ConnectionHelper;
import io.qimia.uhrwerk.common.metastore.config.ConnectionResult;
import io.qimia.uhrwerk.common.metastore.config.ConnectionService;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.ConnectionType;
import io.qimia.uhrwerk.config.ConnectionBuilder;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionDAOTest {

  java.sql.Connection db;
  ConnectionService service;
  String[] connNames = {"S3", "JDBC", "file"};
  private Logger logger;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    db =
            ConnectionHelper.getConnection();
    service = new ConnectionDAO(db);
    logger = Logger.getLogger(this.getClass());
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
    logger.info(result.getNewConnection());
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
    logger.info(result.getNewConnection());
  }

  @Test
  void insertFullConnection() {

    var conns = new ArrayList<Connection>();
    var connection1 =
        (new ConnectionBuilder())
            .name(connNames[0])
            .s3()
            .path("S3Path")
            .secretId("ID")
            .secretKey("key")
            .done()
            .build();
    conns.add(connection1);

    var connection2 =
        (new ConnectionBuilder())
            .name(connNames[1])
            .jdbc()
            .jdbcUrl("url")
            .jdbcDriver("driver")
            .user("user")
            .pass("pass2")
            .done()
            .build();
    conns.add(connection2);

    var connection3 =
        (new ConnectionBuilder()).name(connNames[2]).file().path("filePath").done().build();
    conns.add(connection3);

    for (Connection c : conns) {
      ConnectionResult result = service.save(c, true);
      assertTrue(result.isSuccess());
      assertNotNull(result.getNewConnection());
      assertNotNull(result.getNewConnection().getId());
      logger.info(c);
      logger.info(result.getNewConnection());
    }
  }

  @Test
  void getByNameConnection() {
    try {
      for (String n : connNames) {
        Connection con = service.getByName(n);
        logger.info(con);
        assertEquals(n, con.getName());
      }
    } catch (SQLException e) {
      logger.info("caught in Connection.");
    }
  }

  @Test
  void getByIDConnection() {
    try {
      for (String n : connNames) {
        Connection testCon = new Connection();
        testCon.setName(n);
        testCon.setKey();
        Connection con = service.getById(testCon.getId());
        logger.info(con);
        assertEquals(testCon.getId(), con.getId());
      }
    } catch (SQLException e) {
      logger.info("caught in Connection.");
    }
  }

  @Test
  void getDependency() {}

  @Test
  void getByStatementConnection() {
    try {
      PreparedStatement statement =
          db.prepareStatement("SELECT * FROM CONNECTION WHERE NAME in ();");
      Connection[] con = service.getConnections(statement);
      for (int i = 0; i < con.length; i++) {
        logger.info(con[i]);
        assertEquals(connNames[i], con[i].getName());
      }
    } catch (SQLException e) {
      logger.info("caught in Connection.");
    }
  }
}
