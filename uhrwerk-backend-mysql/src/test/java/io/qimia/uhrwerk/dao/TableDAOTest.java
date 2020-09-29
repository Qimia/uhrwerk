package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.ConnectionHelper;
import io.qimia.uhrwerk.common.metastore.config.ConnectionResult;
import io.qimia.uhrwerk.common.metastore.config.PartitionResult;
import io.qimia.uhrwerk.common.metastore.config.TableResult;
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResult;
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet;
import io.qimia.uhrwerk.common.model.*;
import io.qimia.uhrwerk.config.ConnectionBuilder;
import io.qimia.uhrwerk.config.DependencyBuilder;
import io.qimia.uhrwerk.config.SourceBuilder;
import io.qimia.uhrwerk.config.TableBuilder;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class TableDAOTest {
  private final Logger logger = Logger.getLogger(this.getClass());
  java.sql.Connection db;
  TableDAO tableDAO;
  ConnectionDAO connectionDAO;
  PartitionDAO partitionDAO;

  Source[] generateSources(long tableId) {
    var sources = new Source[2];

    for (int i = 0; i < sources.length; i++) {
      sources[i] = SourceDAOTest.generateSource();
      sources[i].setPath("path" + i);
      sources[i].setTableId(tableId);
      sources[i].setKey();
    }

    return sources;
  }

  private Table generateTable() {
    Table table = new Table();
    table.setArea("dwh");
    table.setVertical("vertical1");
    table.setName("tableA");
    table.setClassName(
        String.join(
            ".", table.getArea(), table.getVertical(), table.getName(), table.getVersion()));
    table.setPartitionUnit(PartitionUnit.MINUTES);
    table.setPartitionSize(15);
    table.setParallelism(8);
    table.setMaxBulkSize(96);
    table.setVersion("1.0");
    table.setPartitioned(true);
    table.setKey();

    table.setSources(generateSources(table.getId()));
    table.setDependencies(generateDependencies(table.getId()));
    table.setTargets(generateTargets(table.getId()));

    return table;
  }

  Dependency[] generateDependencies(long tableId) {
    var dependencies = new Dependency[2];

    for (int i = 0; i < dependencies.length; i++) {
      Table table = new Table();
      table.setArea("staging");
      table.setVertical("vertical2");
      table.setName("tableDep" + i);
      table.setPartitionUnit(PartitionUnit.MINUTES);
      table.setPartitionSize(15);
      table.setParallelism(8);
      table.setMaxBulkSize(96);
      table.setVersion("1.0");
      table.setClassName(
              String.join(
                      ".", table.getArea(), table.getVertical(), table.getName(), table.getVersion()));
      table.setKey();
      tableDAO.save(table, false);
      var target = generateTarget(table.getId(), i);
      new TargetDAO(db).save(new Target[] {target}, target.getTableId(), false);

      dependencies[i] = new Dependency();
      dependencies[i].setTransformType(PartitionTransformType.IDENTITY);
      dependencies[i].setTransformPartitionUnit(PartitionUnit.MINUTES);
      dependencies[i].setTransformPartitionSize(15);
      dependencies[i].setFormat(target.getFormat());
      dependencies[i].setTableName(table.getName());
      dependencies[i].setArea(table.getArea());
      dependencies[i].setVertical(table.getVertical());
      dependencies[i].setVersion(table.getVersion());
      dependencies[i].setTableId(tableId);
      dependencies[i].setKey();
    }

    return dependencies;
  }

  Target generateTarget(long tableId, int i) {
    var target = new Target();
    var connection = SourceDAOTest.generateConnection();
    new ConnectionDAO(db).save(connection, true);

    target.setFormat("csv" + i);
    target.setTableId(tableId);
    target.setConnection(connection);
    target.setKey();

    return target;
  }

  Target[] generateTargets(long tableId) {
    var targets = new Target[2];

    for (int i = 0; i < targets.length; i++) {
      targets[i] = generateTarget(tableId, i);
      targets[i].setKey();
    }

    return targets;
  }

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    this.db = ConnectionHelper.getConnection();
    this.tableDAO = new TableDAO(db);
    this.connectionDAO = new ConnectionDAO(db);
    this.partitionDAO = new PartitionDAO(db);

    // clean
    db.prepareStatement("delete from TABLE_").execute();
    db.prepareStatement("delete from TARGET").execute();
    db.prepareStatement("delete from SOURCE").execute();
    db.prepareStatement("delete from DEPENDENCY").execute();
    db.prepareStatement("delete from CONNECTION").execute();
    db.prepareStatement("delete from PARTITION_").execute();
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    if (db != null) if (!db.isClosed()) db.close();
  }

  @Test
  void saveTable() {
    var table = SourceDAOTest.generateTable();
    table.setPartitioned(false);
    var tableResult = tableDAO.save(table, true);

    assertTrue(tableResult.isSuccess());
    assertFalse(tableResult.isError());
    assertEquals(table, tableResult.getNewResult());
  }

  @Test
  void saveTableWithAllChildren() {
    var table = generateTable();

    var result = tableDAO.save(table, false);

    logger.info(result.getMessage());
    assertTrue(result.isSuccess());
    Arrays.stream(result.getSourceResults())
        .forEach(sourceResult -> assertTrue(sourceResult.isSuccess()));

    assertTrue(result.getTargetResult().isSuccess());

    assertTrue(result.getDependencyResult().isSuccess());

    //    // try to save again unchanged but with only ids in the arrays
    //    var sourceId = table.getSources()[0].getId();
    //    var source = new Source();
    //    source.setId(sourceId);
    //    table.getSources()[0] = source;
    //
    //    var dependencyId = table.getDependencies()[0].getId();
    //    var dependency = new Dependency();
    //    dependency.setId(dependencyId);
    //    table.getDependencies()[0] = dependency;
    //
    //    var targetId = table.getTargets()[0].getId();
    //    var target = new Target();
    //    target.setId(targetId);
    //    table.getTargets()[0] = target;

    // try to save again unchanged but only with connection ids in sources and targets
    var connectionId = table.getSources()[0].getConnection().getId();
    var connection = new Connection();
    connection.setId(connectionId);
    table.getSources()[0].setConnection(connection);

    var targetConnectionId = table.getTargets()[0].getConnection().getId();
    var connectionTarget = new Connection();
    connectionTarget.setId(targetConnectionId);
    table.getTargets()[0].setConnection(connectionTarget);

    var result2 = tableDAO.save(table, false);

    logger.info(result2.getMessage());
    assertTrue(result2.isSuccess());
    Arrays.stream(result2.getSourceResults())
        .forEach(sourceResult -> assertTrue(sourceResult.isSuccess()));

    assertTrue(result2.getTargetResult().isSuccess());

    assertTrue(result2.getDependencyResult().isSuccess());
  }

  @Test
  void savingChangedTableTwiceShouldFail() {
    var table = generateTable();

    var result = tableDAO.save(table, false);

    assertTrue(result.isSuccess());

    table.setPartitioned(false);
    result = tableDAO.save(table, false);

    logger.info(result.getMessage());
    assertFalse(result.isSuccess());
  }

  @Test
  void savingSameTableTwiceShouldSucceed() {
    var table = generateTable();

    var result = tableDAO.save(table, false);

    assertTrue(result.isSuccess());

    result = tableDAO.save(table, false);

    logger.info(result.getMessage());
    assertTrue(result.isSuccess());
  }

  @Test
  void savingChangedTableTwiceShouldSucceedWithOverwrite() {
    var table = generateTable();

    var result = tableDAO.save(table, false);

    assertTrue(result.isSuccess());

    table.setParallelism(9874263);
    result = tableDAO.save(table, true);

    assertTrue(result.isSuccess());
  }

  @Test
  void tableBuilderTest() throws SQLException {
    Connection[] connections =
        new Connection[] {
          (new ConnectionBuilder())
              .name("Test-JDBC-Source1")
              .jdbc()
              .jdbcUrl("url")
              .jdbcDriver("driver")
              .user("user")
              .pass("pass")
              .done()
              .build(),
          (new ConnectionBuilder())
              .name("S3")
              .s3()
              .path("S3Path")
              .secretId("ID")
              .secretKey("key")
              .done()
              .build()
        };

    ConnectionResult[] connResults = connectionDAO.save(connections, true);
    for (int i = 0; i < connResults.length; i++) {
      assertTrue(connResults[i].isSuccess());
      assertNotNull(connResults[i].getNewConnection());
      assertNull(connResults[i].getOldConnection());
    }

    var source1 =
        (new SourceBuilder())
            .connectionName("Test-JDBC-Source1")
            .path("SOURCE_DB.EXT_TABLE1")
            .format("jdbc")
            .version("1.0")
            .partition()
            .unit("minutes")
            .size(30)
            .done()
            .parallelLoad()
            .query("SELECT * FROM SOURCE_DB.EXT_TABLE1")
            .column("Column1")
            .num(8)
            .done()
            .select()
            .query("SELECT * FROM SOURCE_DB.EXT_TABLE1")
            .column("created_ts")
            .done()
            .build();

    Table depTable1 =
        (new TableBuilder())
            .area("TestArea")
            .vertical("TestVertical")
            .table("TestDepTable1")
            .version("1.0")
            .partition()
            .unit("minutes")
            .size(30)
            .done()
            .source(source1)
            .target()
            .connectionName("S3")
            .format("parquet")
            .done()
            .build();
    TableResult depTable1Result = tableDAO.save(depTable1, true);
    assertFalse(depTable1Result.isError());
    assertTrue(depTable1Result.isSuccess());
    assertNotNull(depTable1Result.getNewResult());
    assertNull(depTable1Result.getOldResult());
    assertNotNull(depTable1.getId());
    assertNotNull(depTable1.getTargets()[0].getId());

    var dep1 =
        (new DependencyBuilder())
            .area("TestArea")
            .vertical("TestVertical")
            .table("TestDepTable1")
            .format("parquet")
            .version("1.0")
            .transform()
            .type("aggregate")
            .partition()
            .size(2)
            .done()
            .done()
            .build();

    Table mainTable =
        (new TableBuilder())
            .area("TestArea")
            .vertical("TestVertical")
            .table("TestMainTable")
            .version("1.0")
            .partition()
            .unit("hours")
            .size(1)
            .done()
            .dependency(dep1)
            .target()
            .connectionName("S3")
            .format("parquet")
            .done()
            .build();

    TableResult mainTableResult = tableDAO.save(mainTable, true);
    assertFalse(mainTableResult.isError());
    assertTrue(mainTableResult.isSuccess());
    assertNotNull(mainTableResult.getNewResult());
    assertNull(mainTableResult.getOldResult());

    LocalDateTime start = LocalDateTime.of(2020, 8, 24, 9, 0);
    LocalDateTime end = LocalDateTime.of(2020, 8, 24, 17, 0);
    Duration duration1 = Duration.of(30, ChronoUnit.MINUTES);

    LocalDateTime[] partitionTs = JdbcBackendUtils.getPartitionTs(start, end, duration1);
    for (LocalDateTime ts : partitionTs) {
      Partition partition = new Partition();
      partition.setTargetId(depTable1.getTargets()[0].getId());
      partition.setPartitionTs(ts);
      partition.setKey();
      PartitionResult partitionResult = partitionDAO.save(partition, true);
    }

    Duration duration2 = Duration.of(1, ChronoUnit.HOURS);
    LocalDateTime[] requestedTs = JdbcBackendUtils.getPartitionTs(start, end, duration2);

    TablePartitionResultSet partitionResultSet =
        tableDAO.processingPartitions(mainTable, requestedTs);

    for (int i = 0; i < partitionResultSet.getResolvedTs().length; i++) {
      TablePartitionResult partitionResult = partitionResultSet.getResolved()[i];
      logger.info(partitionResultSet.getResolvedTs()[i]);
      DependencyResult resolvedDependency = partitionResult.getResolvedDependencies()[0];
      for (int j = 0; j < resolvedDependency.getSucceeded().length; j++) {
        logger.info(resolvedDependency.getSucceeded()[j]);
      }
    }
  }

  @Test
  void savingPartitionedSourceForUnpartitionedTableShouldFail() {
    var table = generateTable();
    table.setPartitioned(false);

    var result = tableDAO.save(table, false);

    assertFalse(result.isSuccess());
  }
}
