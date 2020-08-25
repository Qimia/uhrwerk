package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.metastore.config.ConnectionResult;
import io.qimia.uhrwerk.common.metastore.config.TableResult;
import io.qimia.uhrwerk.common.model.*;
import io.qimia.uhrwerk.config.ConnectionBuilder;
import io.qimia.uhrwerk.config.DagBuilder;
import io.qimia.uhrwerk.config.TableBuilder;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class TableDAOTest {

  java.sql.Connection db;
  TableDAO tableDAO;
  ConnectionDAO connectionDAO;
  PartitionDAO partitionDAO;

  Source[] generateSources() {
    var sources = new Source[2];

    for (int i = 0; i < sources.length; i++) {
      sources[i] = SourceDAOTest.generateSource();
      sources[i].setPath("path" + i);
      sources[i].setKey();
    }

    return sources;
  }

  Dependency generateDependency() {
    var dependency = new Dependency();

    dependency.setArea("a");
    dependency.setDependencyTableId(123L);
    dependency.setDependencyTargetId(456L);
    dependency.setFormat("csv");
    dependency.setTableId(SourceDAOTest.generateTable().getId());
    dependency.setTableName("test-table-sourcedao");
    dependency.setTransformPartitionSize(1);
    dependency.setTransformPartitionUnit(PartitionUnit.DAYS);
    dependency.setTransformType(PartitionTransformType.IDENTITY);
    dependency.setVersion("1");
    dependency.setVertical("jjjjj");
    dependency.setKey();

    return dependency;
  }

  Dependency[] generateDependencies() {
    var dependencies = new Dependency[2];

    for (int i = 0; i < dependencies.length; i++) {
      dependencies[i] = generateDependency();
      var table = SourceDAOTest.generateTable();
      table.setName("dependency" + i);
      table.setKey();
      tableDAO.save(table, false);
      dependencies[i].setDependencyTableId(table.getId());
      dependencies[i].setKey();
    }

    return dependencies;
  }

  Target generateTarget() {
    var target = new Target();
    var connection = SourceDAOTest.generateConnection();
    new ConnectionDAO(db).save(connection, true);

    target.setFormat("csv");
    target.setTableId(SourceDAOTest.generateTable().getId());
    target.setConnection(connection);
    target.setKey();

    return target;
  }

  Target[] generateTargets() {
    var targets = new Target[2];

    for (int i = 0; i < targets.length; i++) {
      targets[i] = generateTarget();
      targets[i].setFormat("format" + i);
      targets[i].setKey();
    }

    return targets;
  }

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    this.db =
        DriverManager.getConnection(
            "jdbc:mysql://localhost:53306/UHRWERK_METASTORE", "UHRWERK_USER", "Xq92vFqEKF7TB8H9");
    this.tableDAO = new TableDAO(db);
    this.connectionDAO = new ConnectionDAO(db);
    this.partitionDAO = new PartitionDAO(db);

    // clean
    db.prepareStatement("delete from TABLE_").execute();
    db.prepareStatement("delete from TARGET").execute();
    db.prepareStatement("delete from SOURCE").execute();
    db.prepareStatement("delete from DEPENDENCY").execute();
    db.prepareStatement("delete from CONNECTION").execute();
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void saveTable() {
    var table = SourceDAOTest.generateTable();
    var tableResult = tableDAO.save(table, true);

    assertTrue(tableResult.isSuccess());
    assertFalse(tableResult.isError());
  }

  @Test
  void saveTableWithAllChildren() {
    var table = SourceDAOTest.generateTable();
    table.setSources(generateSources());
    table.setDependencies(generateDependencies());
    table.setTargets(generateTargets());

    var result = tableDAO.save(table, false);

    System.out.println(result.getMessage());
    assertTrue(result.isSuccess());
    Arrays.stream(result.getSourceResults())
        .forEach(sourceResult -> assertTrue(sourceResult.isSuccess()));

    System.out.println(result.getTargetResult().getMessage());
    assertTrue(result.getTargetResult().isSuccess());

    System.out.println(result.getDependencyResult().getMessage());
    assertTrue(result.getDependencyResult().isSuccess());
  }

  @Test
  void tableBuilderTest() throws SQLException {
    Connection[] connections =
        (new ConnectionBuilder())
            .name("JDBC")
            .jdbc()
            .jdbcUrl("url")
            .jdbcDriver("driver")
            .user("user")
            .pass("pass")
            .name("S3")
            .s3()
            .path("S3Path")
            .secretId("ID")
            .secretKey("key")
            .build();

    ConnectionResult[] connResults = connectionDAO.save(connections, true);
    for (int i = 0; i < connResults.length; i++) {
      assertTrue(connResults[i].isSuccess());
      assertNotNull(connResults[i].getNewConnection());
      assertNull(connResults[i].getOldConnection());
    }

    Table mainTable =
        (new TableBuilder())
            .area("TestArea")
            .vertical("TestVertical")
            .table("TestMainTable")
            .version("1.0")
            .partition()
            .unit("hours")
            .size(1)
            .dependency()
            .area("TestArea")
            .vertical("TestVertical")
            .table("TestDepTable1")
            .format("parquet")
            .version("1.0")
            .transform()
            .type("identity")
            .target()
            .connectionName("S3")
            .format("parquet")
            .build();

    TableResult mainTableResult = tableDAO.save(mainTable, true);
    assertFalse(mainTableResult.isError());
    assertTrue(mainTableResult.isSuccess());
    assertNotNull(mainTableResult.getNewResult());
    assertNull(mainTableResult.getOldResult());

    Table depTable1 =
        (new TableBuilder())
            .area("TestArea")
            .vertical("TestVertical")
            .table("TestDepTable1")
            .version("1.0")
            .partition()
            .unit("hours")
            .size(1)
            .source()
            .connectionName("Test-JDBC-Source1")
            .path("SOURCE_DB.EXT_TABLE1")
            .format("JDBC")
            .version("1.0")
            .partition()
            .unit("hours")
            .size(1)
            .parallelLoad()
            .query("SELECT * FROM SOURCE_DB.EXT_TABLE1")
            .column("Column1")
            .num(8)
            .select()
            .query("SELECT * FROM SOURCE_DB.EXT_TABLE1")
            .column("created_ts")
            .target()
            .connectionName("S3")
            .format("parquet")
            .build();
    TableResult depTable1Result = tableDAO.save(depTable1, true);
    assertFalse(depTable1Result.isError());
    assertTrue(depTable1Result.isSuccess());
    assertNotNull(depTable1Result.getNewResult());
    assertNull(depTable1Result.getOldResult());

    LocalDateTime start = LocalDateTime.of(2020, 8, 24, 9, 0);
    LocalDateTime end = LocalDateTime.of(2020, 8, 24, 17, 0);
    Duration duration = Duration.of(30, ChronoUnit.MINUTES);
    LocalDateTime[] partitionTs = JdbcBackendUtils.getPartitionTs(start, end, duration);
    for (LocalDateTime ta : partitionTs) {
      Partition partition = new Partition();
    }
  }
}
