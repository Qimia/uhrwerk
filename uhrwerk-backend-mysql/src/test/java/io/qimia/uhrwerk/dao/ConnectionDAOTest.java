package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.ConnectionResult;
import io.qimia.uhrwerk.common.metastore.config.ConnectionService;
import io.qimia.uhrwerk.common.model.Connection;
import io.qimia.uhrwerk.common.model.ConnectionType;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionDAOTest {

  java.sql.Connection db;
  ConnectionService service;
  String[] connNames = {"S3","JDBC","file"};


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


  @Test
  void insertFullConnection() {
    Connection[] conn = (new io.qimia.uhrwerk.config.ConnectionBuilder())
            .name(connNames[0])
            .s3()
            .path("S3Path")
            .secretId("ID")
            .secretKey("key")
            .name(connNames[1])
            .jdbc()
            .jdbcUrl("url")
            .jdbcDriver("driver")
            .user("user")
            .pass("pass2")
            .name(connNames[2])
            .file()
            .path("filePath")
            .build();

    for (Connection c : conn)
    {
      ConnectionResult result = service.save(c, true);
      assertTrue(result.isSuccess());
      assertNotNull(result.getNewConnection());
      assertNotNull(result.getNewConnection().getId());
      System.out.println(c);
      System.out.println(result.getNewConnection());
    }
  }

  @Test
  void getByNameConnection() {
    try {
      for (String n : connNames) {
        Connection con = service.getByName(n);
        System.out.println(con);
        assertEquals(n,con.getName());
      }
    } catch (SQLException e) {
      System.out.println("caught in Connection.");
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
        System.out.println(con);
        assertEquals(testCon.getId(),con.getId());
      }
    } catch (SQLException e) {
      System.out.println("caught in Connection.");
    }

  }


  @Test
  void getDependency() {

  }

  @Test
  void getByStatementConnection() {
    try {
        PreparedStatement statement = db.prepareStatement("SELECT * FROM CONNECTION WHERE NAME in ();");
        Connection[] con = service.getConnections(statement);
      for (int i=0;i<con.length; i++) {
        System.out.println(con[i]);
        assertEquals(connNames[i],con[i].getName());
      }
    } catch (SQLException e) {
      System.out.println("caught in Connection.");
    }

  }

}
