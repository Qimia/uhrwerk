package io.qimia.uhrwerk.backend.dao.config;

import io.qimia.uhrwerk.config.PartitionTemporalType;
import io.qimia.uhrwerk.config.model.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TableDAOTest {
  Connection db;

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
    Table table = new Table();
    table.setArea("test-area");
    table.setVertical("test-vertical");
    table.setName("test-table");
    table.setPartitionSizeType(PartitionTemporalType.MINUTES);
    table.setPartitionSizeInt(15);
    table.setParallelism(8);
    table.setMaxBatches(96);
    table.setVersion("1.0");
    Long id = TableDAO.save(db, table);
    assertNotNull(id);

    System.out.println("Wrote Config Table. id :\n" + id);

    Table dbTable = TableDAO.get(db, id);

    assertNotNull(dbTable);
    assertEquals(id, dbTable.getId());

    System.out.println("Got Config Table :\n" + dbTable);
  }
}
