package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.ConnectionService;
import io.qimia.uhrwerk.common.model.PartitionUnit;
import io.qimia.uhrwerk.common.model.Table;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class TableDAOTest {

  java.sql.Connection db;
  TableDAO service;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    db =
        DriverManager.getConnection(
            "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
    service = new TableDAO(db);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void save() {}

  @Test
  void saveTable() throws SQLException {
    Table table = new Table();
    table.setArea("test-area");
    table.setVertical("test-vertical");
    table.setName("test-table");
    table.setPartitionUnit(PartitionUnit.MINUTES);
    table.setPartitionSize(15);
    table.setParallelism(8);
    table.setMaxBulkSize(96);
    table.setVersion("1.0");
    Long id = service.save(table);
  }

  @Test
  void get() {}
}
