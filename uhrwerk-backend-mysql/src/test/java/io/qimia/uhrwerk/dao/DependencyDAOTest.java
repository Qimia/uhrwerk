package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.ConnectionHelper;
import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class DependencyDAOTest {

  java.sql.Connection db;

  @org.junit.jupiter.api.BeforeEach
  void setUp() throws SQLException {
    db = ConnectionHelper.getConnection();

    // Setup 2 tables to depend on with targets (which have a connection)
    var conn = new Connection();
    conn.setName("a_connection");
    conn.setKey();
    Statement a = db.createStatement();
    a.executeUpdate(
        "INSERT INTO CONNECTION (id, name, type, path) VALUES ("
            + conn.getId()
            + ", 'a_connection', 'fs', '/some/test/path')");
    a.close();

    var tableDepA = new Table();
    tableDepA.setArea("area1");
    tableDepA.setVertical("vertical1");
    tableDepA.setName("name1");
    tableDepA.setVersion("1.0");
    tableDepA.setClassName(
        String.join(
            ".",
            tableDepA.getArea(),
            tableDepA.getVertical(),
            tableDepA.getName(),
            tableDepA.getVersion()));
    tableDepA.setPartitioned(true);
    tableDepA.setPartitionSize(1);
    tableDepA.setPartitionUnit(PartitionUnit.HOURS);
    tableDepA.setKey();
    Statement b = db.createStatement();
    b.executeUpdate(
        "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
            + "VALUES ("
            + tableDepA.getId()
            + ", 'area1', 'vertical1', 'name1', '1.0', TRUE, 'HOURS', 1, 1, 1, 'area1.vertical1.name1.1.0')");
    b.close();

    var depATarget = new Target();
    depATarget.setTableId(tableDepA.getId());
    depATarget.setFormat("parquet");
    depATarget.setKey();
    Statement c = db.createStatement();
    c.executeUpdate(
        "INSERT INTO TARGET (id, table_id, connection_id, format)"
            + "VALUES ("
            + depATarget.getId()
            + ","
            + tableDepA.getId()
            + ","
            + conn.getId()
            + ", 'parquet')");
    c.close();

    var tableDepB = new Table();
    tableDepB.setArea("area1");
    tableDepB.setVertical("vertical1");
    tableDepB.setName("name2");
    tableDepB.setVersion("1.0");
    tableDepB.setClassName(
        String.join(
            ".",
            tableDepB.getArea(),
            tableDepB.getVertical(),
            tableDepB.getName(),
            tableDepB.getVersion()));
    tableDepB.setPartitioned(true);
    tableDepB.setPartitionSize(1);
    tableDepB.setPartitionUnit(PartitionUnit.HOURS);
    tableDepB.setKey();
    Statement d = db.createStatement();
    d.executeUpdate(
        "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
            + "VALUES ("
            + tableDepB.getId()
            + ", 'area1', 'vertical1', 'name2', '1.0', TRUE, 'HOURS', 1, 1, 1, 'area1.vertical1.name2.1.0')");
    d.close();

    var depBTarget = new Target();
    depBTarget.setTableId(tableDepB.getId());
    depBTarget.setFormat("parquet");
    depBTarget.setKey();
    Statement e = db.createStatement();
    e.executeUpdate(
        "INSERT INTO TARGET (id, table_id, connection_id, format)"
            + "VALUES ("
            + depBTarget.getId()
            + ","
            + tableDepB.getId()
            + ","
            + conn.getId()
            + ", 'parquet')");
    e.close();

    var tableDepC = new Table();
    tableDepC.setArea("area1");
    tableDepC.setVertical("vertical1");
    tableDepC.setName("name3");
    tableDepC.setVersion("1.0");
    tableDepC.setPartitioned(false);
    tableDepC.setKey();
    Statement f = db.createStatement();
    f.executeUpdate(
        "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, parallelism, max_bulk_size, class_name)"
            + "VALUES ("
            + tableDepC.getId()
            + ", 'area1', 'vertical1', 'name3', '1.0', FALSE, 1, 1, 'area1.vertical1.name3.1.0')");
    f.close();

    var depCTarget = new Target();
    depCTarget.setTableId(tableDepC.getId());
    depCTarget.setFormat("parquet");
    depCTarget.setKey();
    Statement g = db.createStatement();
    g.executeUpdate(
        "INSERT INTO TARGET (id, table_id, connection_id, format)"
            + "VALUES ("
            + depCTarget.getId()
            + ","
            + tableDepC.getId()
            + ","
            + conn.getId()
            + ", 'parquet')");
    g.close();
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() throws SQLException {
    // WARNING deletes all data as cleanup
    var deleteDependencyStm = db.createStatement();
    deleteDependencyStm.execute("DELETE FROM DEPENDENCY");
    deleteDependencyStm.close();
    var deleteSourceStm = db.createStatement(); // In case of some lost source data
    deleteSourceStm.execute("DELETE FROM SOURCE");
    deleteSourceStm.close();
    var deleteTargetStm = db.createStatement();
    deleteTargetStm.execute("DELETE FROM TARGET");
    deleteTargetStm.close();
    var deleteConnectionStm = db.createStatement();
    deleteConnectionStm.execute("DELETE FROM CONNECTION");
    deleteConnectionStm.close();
    var deleteTableStm = db.createStatement();
    deleteTableStm.execute("DELETE FROM TABLE_");
    deleteTableStm.close();
    if (db != null) if (!db.isClosed()) db.close();
  }

  /**
   * Setup function which creates a new Table (which can be used to attach dependencies to) and
   * inserts this table already. This should mimic the TableDAO setup before calling DependencyDAO
   *
   * @return A Table object present in the MetaStore
   * @throws SQLException
   */
  Table insertTestTable() throws SQLException {
    var newTable = new Table();
    newTable.setArea("area1");
    newTable.setVertical("vertical1");
    newTable.setName("name4");
    newTable.setVersion("1.0");
    newTable.setClassName(
        String.join(
            ".",
            newTable.getArea(),
            newTable.getVertical(),
            newTable.getName(),
            newTable.getVersion()));
    newTable.setPartitioned(true);
    newTable.setPartitionUnit(PartitionUnit.HOURS);
    newTable.setPartitionSize(1);
    newTable.setKey();
    Statement setupTableStm = db.createStatement();
    setupTableStm.executeUpdate(
        "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
            + "VALUES ("
            + newTable.getId()
            + ", 'area1', 'vertical1', 'name4', '1.0', TRUE, 'HOURS', 1, 1, 1, 'area1.vertical1.name4.1.0')");
    setupTableStm.close();
    return newTable;
  }

  /**
   * Create two dependency objects which are set to the same values as 2 already present tables in
   * the DB
   *
   * @param tableId Id from the table for which the dependencies will be set
   * @return array of dependency objects
   */
  Dependency[] createThreeDependencies(long tableId) {
    var depA = new Dependency();
    depA.setArea("area1");
    depA.setVertical("vertical1");
    depA.setTableName("name1");
    depA.setVersion("1.0");
    depA.setFormat("parquet");
    depA.setTransformType(PartitionTransformType.IDENTITY);
    depA.setTransformPartitionSize(1);
    depA.setTableId(tableId);
    depA.setKey();
    var depB = new Dependency();
    depB.setArea("area1");
    depB.setVertical("vertical1");
    depB.setTableName("name2");
    depB.setVersion("1.0");
    depB.setFormat("parquet");
    depB.setTransformType(PartitionTransformType.WINDOW);
    depB.setTransformPartitionSize(2);
    depB.setTableId(tableId);
    depB.setKey();
    var depC = new Dependency();
    depC.setArea("area1");
    depC.setVertical("vertical1");
    depC.setTableName("name3");
    depC.setVersion("1.0");
    depC.setFormat("parquet");
    depC.setTransformType(PartitionTransformType.NONE);
    depC.setTableId(tableId);
    depC.setKey();
    return new Dependency[] {depA, depB, depC};
  }

  Dependency[] createBadDependency(long tableId) {
    var depA = new Dependency();
    depA.setArea("area1");
    depA.setVertical("vertical1");
    depA.setTableName("badtable");
    depA.setVersion("10.x");
    depA.setFormat("jdbc");
    depA.setTransformType(PartitionTransformType.IDENTITY);
    depA.setTransformPartitionSize(1);
    depA.setTableId(tableId);
    depA.setKey();
    return new Dependency[] {depA};
  }

  @Test
  void findCorrectTest() throws SQLException {
    Table newTable = insertTestTable();
    Dependency[] dependencies = createThreeDependencies(newTable.getId());
    Dependency[] partitionedDependency = new Dependency[] {dependencies[0], dependencies[1]};
    newTable.setDependencies(partitionedDependency);

    DependencyDAO dao = new DependencyDAO(db);
    DependencyDAO.FindTableRes checkRes = dao.findTables(newTable.getDependencies());
    assertTrue(checkRes.missingNames.isEmpty());
    for (DependencyDAO.TablePartRes res : checkRes.foundTables) {
      assertEquals(PartitionUnit.HOURS, res.partitionUnit);
      assertEquals(1, res.partitionSize);
    }
  }

  @Test
  void notFindIncorrectTest() throws SQLException {
    Table newTable = insertTestTable();
    Dependency[] dependencies = createBadDependency(newTable.getId());
    newTable.setDependencies(dependencies);

    DependencyDAO dao = new DependencyDAO(db);
    DependencyDAO.FindTableRes checkRes = dao.findTables(newTable.getDependencies());
    assertTrue(checkRes.missingNames.contains(dependencies[0].getTableName()));
  }

  @Test
  void addAndDeleteTest() throws SQLException {
    Table newTable = insertTestTable();
    Dependency[] dependencies = createThreeDependencies(newTable.getId());
    Set<String> tableNames =
        Arrays.stream(dependencies)
            .map(Dependency::getTableName)
            .collect(Collectors.toCollection(HashSet::new));
    newTable.setDependencies(dependencies);

    DependencyDAO dao = new DependencyDAO(db);
    dao.insertDependencies(dependencies);

    Dependency[] storedDependencies = dao.get(newTable.getId());
    assertEquals(3, storedDependencies.length);
    for (Dependency d : storedDependencies) {
      assertTrue(tableNames.contains(d.getTableName()));
    }

    dao.deleteAllDependencies(newTable.getId());
    storedDependencies = dao.get(newTable.getId());
    assertEquals(0, storedDependencies.length);
  }

  @Test
  void testDependencyChecking() throws SQLException {
    Table newTable = insertTestTable();
    Dependency[] dependencies = createThreeDependencies(newTable.getId());
    newTable.setDependencies(dependencies);

    DependencyDAO dao = new DependencyDAO(db);
    DependencyDAO.FindTableRes checkRes = dao.findTables(newTable.getDependencies());
    var testRes1 =
        DependencyDAO.checkPartitionSizes(
            dependencies,
            newTable.getPartitionUnit(),
            newTable.getPartitionSize(),
            checkRes.foundTables);
    assertTrue(testRes1.success);

    ArrayList<DependencyDAO.TablePartRes> tablePartitionInfo = new ArrayList<>();
    var tableA = new DependencyDAO.TablePartRes();
    tableA.tableId = dependencies[0].getDependencyTableId();
    tableA.partitionUnit = PartitionUnit.HOURS;
    tableA.partitionSize = 1;
    tablePartitionInfo.add(tableA);
    var tableB = new DependencyDAO.TablePartRes();
    tableB.tableId = dependencies[1].getDependencyTableId();
    tableB.partitionUnit = PartitionUnit.MINUTES;
    tableB.partitionSize = 15;
    tablePartitionInfo.add(tableB);

    var testRes2 =
        DependencyDAO.checkPartitionSizes(
            dependencies,
            newTable.getPartitionUnit(),
            newTable.getPartitionSize(),
            tablePartitionInfo);
    assertFalse(testRes2.success);
    assertEquals(dependencies[1].getTableName(), testRes2.badTableNames[0]);
  }

  @Test
  void basicFullSaveTest() throws SQLException {
    Table newTable = insertTestTable();
    Dependency[] dependencies = createThreeDependencies(newTable.getId());
    newTable.setDependencies(dependencies);

    DependencyDAO dao = new DependencyDAO(db);
    var saveRes = dao.save(newTable, true);
    assertTrue(saveRes.isSuccess());
    assertFalse(saveRes.isError());
    assertEquals(3, saveRes.getDependenciesSaved().length);

    var foundDependencies = dao.get(newTable.getId());
    assertEquals(3, foundDependencies.length);

    assertEquals("area1", foundDependencies[2].getArea());
    assertEquals("vertical1", foundDependencies[2].getVertical());
    assertEquals("name1", foundDependencies[2].getTableName());
    assertEquals("1.0", foundDependencies[2].getVersion());
    assertEquals("parquet", foundDependencies[2].getFormat());
    assertEquals(PartitionTransformType.IDENTITY, foundDependencies[2].getTransformType());
    assertEquals(1, foundDependencies[2].getTransformPartitionSize());
    assertEquals(newTable.getId(), foundDependencies[2].getTableId());
  }

  @Test
  void testStoredDependenciesAfterSaving() throws SQLException {
    // relies on most of the sub-methods
    Table newTable = insertTestTable();
    Dependency[] dependencies = createThreeDependencies(newTable.getId());
    newTable.setDependencies(dependencies);

    DependencyDAO dao = new DependencyDAO(db);
    // First use normal save method to store dependencies
    var saveRes = dao.save(newTable, true);
    assertTrue(saveRes.isSuccess());

    var checkRes = dao.checkExistingDependencies(newTable.getId(), dependencies);
    // Then see if a ExistingDependencyCheck would find them and say that they are the same
    assertTrue(checkRes.found);
    assertTrue(checkRes.correct);
  }

  @Test
  void letUnpartitionedDependonPartitioned() throws SQLException {
    // This should be stopped at storing

    var unpartitionedTable = new Table();
    unpartitionedTable.setArea("area1");
    unpartitionedTable.setVertical("vertical1");
    unpartitionedTable.setName("name5");
    unpartitionedTable.setVersion("1.0");
    unpartitionedTable.setClassName(
        String.join(
            ".",
            unpartitionedTable.getArea(),
            unpartitionedTable.getVertical(),
            unpartitionedTable.getName(),
            unpartitionedTable.getVersion()));
    unpartitionedTable.setPartitioned(false);
    unpartitionedTable.setKey();
    Statement setupTableStm = db.createStatement();
    setupTableStm.executeUpdate(
        "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, parallelism, max_bulk_size, class_name)"
            + "VALUES ("
            + unpartitionedTable.getId()
            + ", 'area1', 'vertical1', 'name5', '1.0', FALSE, 1, 1, 'area1.vertical1.name5.1.0')");
    setupTableStm.close();

    var depA = new Dependency();
    depA.setArea("area1");
    depA.setVertical("vertical1");
    depA.setTableName("name1");
    depA.setVersion("1.0");
    depA.setFormat("parquet");
    depA.setTransformType(PartitionTransformType.IDENTITY);
    depA.setTransformPartitionSize(1);
    depA.setTableId(unpartitionedTable.getId());
    depA.setKey();
    var dependencies = new Dependency[] {depA};
    unpartitionedTable.setDependencies(dependencies);

    DependencyDAO dao = new DependencyDAO(db);
    // First use normal save method to store dependencies
    var saveRes = dao.save(unpartitionedTable, true);
    assertFalse(saveRes.isSuccess());

    depA.setTransformType(PartitionTransformType.NONE);
    var saveRes2 = dao.save(unpartitionedTable, true);
    assertFalse(saveRes2.isSuccess());
  }

  @Test
  void checkKeyGeneration() {
    var tableDepA = new Table();
    tableDepA.setArea("area1");
    tableDepA.setVertical("vertical1");
    tableDepA.setName("name1");
    tableDepA.setVersion("1.0");
    tableDepA.setKey();
    var depATarget = new Target();
    depATarget.setTableId(tableDepA.getId());
    depATarget.setFormat("parquet");
    depATarget.setKey();

    var tableB = new Table();
    tableB.setArea("area1");
    tableB.setVertical("vertical1");
    tableB.setName("name2");
    tableB.setVersion("1.0");
    tableB.setKey();
    var depA = new Dependency();
    depA.setArea("area1");
    depA.setVertical("vertical1");
    depA.setTableName("name1");
    depA.setVersion("1.0");
    depA.setFormat("parquet");
    depA.setTableId(tableB.getId());
    depA.setKey();
    assertEquals(depATarget.getId(), depA.getDependencyTargetId());
  }

  @Test
  void compareDependencies() {
    var table = new Table();
    table.setArea("area1");
    table.setVertical("vertical1");
    table.setName("name2");
    table.setVersion("1.0");
    table.setKey();

    var depA = new Dependency();
    depA.setArea("area1");
    depA.setVertical("vertical1");
    depA.setTableName("name1");
    depA.setVersion("1.0");
    depA.setFormat("parquet");
    depA.setTransformType(PartitionTransformType.IDENTITY);
    depA.setTransformPartitionSize(1);
    depA.setTableId(table.getId());
    depA.setKey();

    var depB = new Dependency();
    depB.setArea("area1");
    depB.setVertical("vertical1");
    depB.setTableName("name1");
    depB.setVersion("1.0");
    depB.setFormat("parquet");
    depB.setTransformType(PartitionTransformType.IDENTITY);
    depB.setTransformPartitionSize(1);
    depB.setTableId(table.getId());
    depB.setKey();

    var res1 = DependencyDAO.compareDependency(depA, depB);
    assertTrue(res1.success);

    depA.setTransformType(PartitionTransformType.WINDOW);
    depA.setTransformPartitionSize(2);
    var res2 = DependencyDAO.compareDependency(depA, depB);
    assertFalse(res2.success);
    assertTrue(res2.problem.contains("type"));
  }
}
