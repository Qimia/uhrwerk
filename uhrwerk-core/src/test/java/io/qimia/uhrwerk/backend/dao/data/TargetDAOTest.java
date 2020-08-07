package io.qimia.uhrwerk.backend.dao.data;

import io.qimia.uhrwerk.backend.dao.config.ConnectionDAO;
import io.qimia.uhrwerk.backend.dao.config.TableDAO;
import io.qimia.uhrwerk.backend.model.BatchTemporalUnit;
import io.qimia.uhrwerk.backend.model.config.Table;
import io.qimia.uhrwerk.backend.model.data.Target;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TargetDAOTest {
  Connection db;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws ClassNotFoundException, SQLException {
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

    db.setAutoCommit(false);
    Table table = new Table();
    table.setArea("test-area");
    table.setVertical("test-vertical");
    table.setTableName("test-table");
    table.setBatchTemporalUnit(BatchTemporalUnit.MINUTES);
    table.setBatchSize(15);
    table.setParallelism(8);
    table.setMaxPartitions(96);
    table.setVersion("1.0");
    Long cfTableId = TableDAO.save(db, table);
    assertNotNull(cfTableId);

    io.qimia.uhrwerk.backend.model.config.Connection conn =
        new io.qimia.uhrwerk.backend.model.config.Connection();
    conn.setConnectionName("Test-Conn1");
    conn.setConnectionUrl("Test-URl");
    conn.setConnectionType("Test-Type");
    conn.setVersion("1.0");
    Long cfConnectionId = ConnectionDAO.save(db, conn);
    assertNotNull(cfConnectionId);

    System.out.println("Wrote Config Table. id :\n" + cfTableId);

    List<Target> targets = new ArrayList<>(2);
    for (int i = 0; i < 2; i++) {
      Target target = new Target();
      target.setCfTableId(cfTableId);
      target.setCfConnectionId(cfConnectionId);
      target.setPath("test-path-" + i);
      targets.add(target);
    }
    List<Long> targetIds = TargetDAO.save(db, targets);
    assertEquals(2, targetIds.size());
    db.commit();
  }
}
