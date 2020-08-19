package io.qimia.uhrwerk.dao;

import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
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
        db = DriverManager.getConnection(
                "jdbc:mysql://localhost:53306/UHRWERK_METASTORE",
                "UHRWERK_USER",
                "Xq92vFqEKF7TB8H9"
        );

        var conn = new Connection();
        conn.setName("a_connection");
        conn.setKey();
        Statement a = db.createStatement();
        a.executeUpdate("INSERT INTO CONNECTION (id, name, type, path) VALUES (" + conn.getId() +
                ", 'a_connection', 'fs', '/some/test/path')");
        a.close();

        var tableDepA = new Table();
        tableDepA.setArea("area1");
        tableDepA.setVertical("vertical1");
        tableDepA.setName("name1");
        tableDepA.setVersion("1.0");
        tableDepA.setKey();
        Statement b = db.createStatement();
        b.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + tableDepA.getId() + ", 'area1', 'vertical1', 'name1', '1.0', 'HOURS', 1, 1, 1)");
        b.close();

        var depATarget = new Target();
        depATarget.setTableId(tableDepA.getId());
        depATarget.setFormat("parquet");
        depATarget.setKey();
        Statement c = db.createStatement();
        c.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
                "VALUES (" + depATarget.getId() + "," + tableDepA.getId() + "," + conn.getId() + ", 'parquet')");
        c.close();

        var tableDepB = new Table();
        tableDepB.setArea("area1");
        tableDepB.setVertical("vertical1");
        tableDepB.setName("name2");
        tableDepB.setVersion("1.0");
        tableDepB.setKey();
        Statement d = db.createStatement();
        d.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + tableDepB.getId() + ", 'area1', 'vertical1', 'name2', '1.0', 'HOURS', 1, 1, 1)");
        d.close();

        var depBTarget = new Target();
        depBTarget.setTableId(tableDepB.getId());
        depBTarget.setFormat("parquet");
        depBTarget.setKey();
        Statement e = db.createStatement();
        e.executeUpdate("INSERT INTO TARGET (id, table_id, connection_id, format)" +
                "VALUES (" + depBTarget.getId() + "," + tableDepB.getId() + "," + conn.getId() + ", 'parquet')");
        e.close();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() throws SQLException {
        var deleteDependencyStm = db.createStatement();
        deleteDependencyStm.execute("DELETE FROM DEPENDENCY");
        deleteDependencyStm.close();
        var deleteSourceStm = db.createStatement();  // In case of some lost source data
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

    Table insertTestTable() throws SQLException {
        var newTable = new Table();
        newTable.setArea("area1");
        newTable.setVertical("vertical1");
        newTable.setName("name3");
        newTable.setVersion("1.0");
        newTable.setPartitionUnit(PartitionUnit.HOURS);
        newTable.setPartitionSize(1);
        newTable.setKey();
        Statement setupTableStm = db.createStatement();
        setupTableStm.executeUpdate("INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size)" +
                "VALUES (" + newTable.getId() + ", 'area1', 'vertical1', 'name3', '1.0', 'HOURS', 1, 1, 1)");
        setupTableStm.close();
        return newTable;
    }

    Dependency[] createTwoDependencies(long tableId) {
        var depA = new Dependency();
        depA.setArea("area1");
        depA.setVertical("vertical1");
        depA.setTableName("name1");
        depA.setVersion("1.0");
        depA.setFormat("parquet");
        depA.setTransformType(PartitionTransformType.IDENTITY);
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
        return new Dependency[] {depA, depB};
    }

    Dependency[] createBadDependency(long tableId) {
        var depA = new Dependency();
        depA.setArea("area1");
        depA.setVertical("vertical1");
        depA.setTableName("badtable");
        depA.setVersion("10.x");
        depA.setFormat("jdbc");
        depA.setTransformType(PartitionTransformType.IDENTITY);
        depA.setTableId(tableId);
        depA.setKey();
        return new Dependency[] {depA};
    }

    @Test
    void findCorrectTest() throws SQLException {
        Table newTable = insertTestTable();
        Dependency[] dependencies = createTwoDependencies(newTable.getId());
        newTable.setDependencies(dependencies);

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
        Dependency[] dependencies = createTwoDependencies(newTable.getId());
        Set<String> tableNames = Arrays.stream(dependencies).map(Dependency::getTableName).collect(Collectors.toCollection(HashSet::new));
        newTable.setDependencies(dependencies);

        DependencyDAO dao = new DependencyDAO(db);
        dao.insertDependencies(dependencies);

        Dependency[] storedDependencies = dao.get(newTable.getId());
        assertEquals(2, storedDependencies.length);
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
        Dependency[] dependencies = createTwoDependencies(newTable.getId());
        newTable.setDependencies(dependencies);

        DependencyDAO dao = new DependencyDAO(db);
        DependencyDAO.FindTableRes checkRes = dao.findTables(newTable.getDependencies());
        var testRes1 = DependencyDAO.checkPartitionSizes(
                dependencies,
                newTable.getPartitionUnit(),
                newTable.getPartitionSize(),
                checkRes.foundTables
        );
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

        var testRes2 = DependencyDAO.checkPartitionSizes(
                dependencies,
                newTable.getPartitionUnit(),
                newTable.getPartitionSize(),
                tablePartitionInfo
        );
        assertFalse(testRes2.success);
        assertEquals(dependencies[1].getTableName(), testRes2.badTableNames[0]);
    }

    @Test
    void basicFullSaveTest() throws SQLException {
        Table newTable = insertTestTable();
        Dependency[] dependencies = createTwoDependencies(newTable.getId());
        newTable.setDependencies(dependencies);

        DependencyDAO dao = new DependencyDAO(db);
        var saveRes = dao.save(dependencies, newTable.getId(), newTable.getPartitionUnit(),
                newTable.getPartitionSize(), true);
        assertTrue(saveRes.isSuccess());
        assertFalse(saveRes.isError());
        assertEquals(2, saveRes.getDependenciesSaved().length);

        var foundDependencies = dao.get(newTable.getId());
        assertEquals(2, saveRes.getDependenciesSaved().length);
    }

    @Test
    void testStoredDependenciesAfterSaving() throws SQLException {
        // relies on most of the sub-methods
        Table newTable = insertTestTable();
        Dependency[] dependencies = createTwoDependencies(newTable.getId());
        newTable.setDependencies(dependencies);

        DependencyDAO dao = new DependencyDAO(db);
        var saveRes = dao.save(dependencies, newTable.getId(), newTable.getPartitionUnit(),
                newTable.getPartitionSize(), true);
        assertTrue(saveRes.isSuccess());

        var checkRes = dao.checkExistingDependencies(newTable.getId(), dependencies);
        assertTrue(checkRes.found);
//        assertTrue(checkRes.correct); TODO: First fix Dependency's transformPartitionSize default db value
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
        depA.setTableId(table.getId());
        depA.setKey();

        var depB = new Dependency();
        depB.setArea("area1");
        depB.setVertical("vertical1");
        depB.setTableName("name1");
        depB.setVersion("1.0");
        depB.setFormat("parquet");
        depB.setTransformType(PartitionTransformType.IDENTITY);
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
