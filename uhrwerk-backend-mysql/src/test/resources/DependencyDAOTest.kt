import io.qimia.uhrwerk.ConnectionHelper
import io.qimia.uhrwerk.common.metastore.model.*
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.dao.DependencyDAO.Companion.checkPartitionSizes
import io.qimia.uhrwerk.dao.DependencyDAO.Companion.compareDependency
import io.qimia.uhrwerk.dao.DependencyDAO.TablePartRes
import io.qimia.uhrwerk.repo.DependencyRepo
import org.junit.jupiter.api.*
import java.sql.Connection
import java.sql.SQLException
import java.util.*
import java.util.function.Supplier
import java.util.stream.Collectors

class DependencyDAOTest {
    var db: Connection? = null

    @BeforeEach
    @Throws(SQLException::class)
    fun setUp() {
        db = ConnectionHelper.getConnecion()

        // Setup 2 tables to depend on with targets (which have a connection)
        val conn = ConnectionModel.builder()
            .name("a_connection").build()
        val a = db!!.createStatement()
        a.executeUpdate(
            "INSERT INTO CONNECTION (id, name, type, path) VALUES ("
                    + conn.id
                    + ", 'a_connection', 'fs', '/some/test/path')"
        )
        a.close()
        val tableDepA = TableModel.builder().build();
        tableDepA.area = "area1"
        tableDepA.vertical = "vertical1"
        tableDepA.name = "name1"
        tableDepA.version = "1.0"
        tableDepA.className = java.lang.String.join(
            ".",
            tableDepA.area,
            tableDepA.vertical,
            tableDepA.name,
            tableDepA.version
        )
        tableDepA.isPartitioned = true
        tableDepA.partitionSize = 1
        tableDepA.partitionUnit = PartitionUnit.HOURS

        tableDepA.setKey()
        val b = db!!.createStatement()
        b.executeUpdate(
            "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
                    + "VALUES ("
                    + tableDepA.id
                    + ", 'area1', 'vertical1', 'name1', '1.0', TRUE, 'HOURS', 1, 1, 1, 'area1.vertical1.name1.1.0')"
        )
        b.close()
        val depATarget = Target()
        depATarget.tableId = tableDepA.id
        depATarget.format = "parquet"
        depATarget.setKey()
        val c = db!!.createStatement()
        c.executeUpdate(
            "INSERT INTO TARGET (id, table_id, connection_id, format)"
                    + "VALUES ("
                    + depATarget.id
                    + ","
                    + tableDepA.id
                    + ","
                    + conn.id
                    + ", 'parquet')"
        )
        c.close()
        val tableDepB = TableModel.builder().build();
        tableDepB.area = "area1"
        tableDepB.vertical = "vertical1"
        tableDepB.name = "name2"
        tableDepB.version = "1.0"
        tableDepB.className = java.lang.String.join(
            ".",
            tableDepB.area,
            tableDepB.vertical,
            tableDepB.name,
            tableDepB.version
        )
        tableDepB.isPartitioned = true
        tableDepB.partitionSize = 1
        tableDepB.partitionUnit = PartitionUnit.HOURS
        tableDepB.setKey()
        val d = db!!.createStatement()
        d.executeUpdate(
            "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
                    + "VALUES ("
                    + tableDepB.id
                    + ", 'area1', 'vertical1', 'name2', '1.0', TRUE, 'HOURS', 1, 1, 1, 'area1.vertical1.name2.1.0')"
        )
        d.close()
        val depBTarget = Target()
        depBTarget.tableId = tableDepB.id
        depBTarget.format = "parquet"
        depBTarget.setKey()
        val e = db!!.createStatement()
        e.executeUpdate(
            "INSERT INTO TARGET (id, table_id, connection_id, format)"
                    + "VALUES ("
                    + depBTarget.id
                    + ","
                    + tableDepB.id
                    + ","
                    + conn.id
                    + ", 'parquet')"
        )
        e.close()
        val tableDepC = TableModel.builder().build();
        tableDepC.area = "area1"
        tableDepC.vertical = "vertical1"
        tableDepC.name = "name3"
        tableDepC.version = "1.0"
        tableDepC.isPartitioned = false
        tableDepC.setKey()
        val f = db!!.createStatement()
        f.executeUpdate(
            "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, parallelism, max_bulk_size, class_name)"
                    + "VALUES ("
                    + tableDepC.id
                    + ", 'area1', 'vertical1', 'name3', '1.0', FALSE, 1, 1, 'area1.vertical1.name3.1.0')"
        )
        f.close()
        val depCTarget = Target()
        depCTarget.tableId = tableDepC.id
        depCTarget.format = "parquet"
        depCTarget.setKey()
        val g = db!!.createStatement()
        g.executeUpdate(
            "INSERT INTO TARGET (id, table_id, connection_id, format)"
                    + "VALUES ("
                    + depCTarget.id
                    + ","
                    + tableDepC.id
                    + ","
                    + conn.id
                    + ", 'parquet')"
        )
        g.close()
    }

    @AfterEach
    @Throws(SQLException::class)
    fun tearDown() {
        // WARNING deletes all data as cleanup
        val deleteDependencyStm = db!!.createStatement()
        deleteDependencyStm.execute("DELETE FROM DEPENDENCY")
        deleteDependencyStm.close()
        val deleteSourceStm = db!!.createStatement() // In case of some lost source data
        deleteSourceStm.execute("DELETE FROM SOURCE")
        deleteSourceStm.close()
        val deleteTargetStm = db!!.createStatement()
        deleteTargetStm.execute("DELETE FROM TARGET")
        deleteTargetStm.close()
        val deleteConnectionStm = db!!.createStatement()
        deleteConnectionStm.execute("DELETE FROM CONNECTION")
        deleteConnectionStm.close()
        val deleteTableStm = db!!.createStatement()
        deleteTableStm.execute("DELETE FROM TABLE_")
        deleteTableStm.close()
        if (db != null) {
            if (!db!!.isClosed) {
                db!!.close()
            }
        }
    }

    /**
     * Setup function which creates a new Table (which can be used to attach dependencies to) and
     * inserts this table already. This should mimic the TableDAO setup before calling DependencyDAO
     *
     * @return A Table object present in the MetaStore
     * @throws SQLException
     */
    @Throws(SQLException::class)
    fun insertTestTable(): TableModel {
        val newTable = TableModel.builder().build();
        newTable.area = "area1"
        newTable.vertical = "vertical1"
        newTable.name = "name4"
        newTable.version = "1.0"
        newTable.className = java.lang.String.join(
            ".",
            newTable.area,
            newTable.vertical,
            newTable.name,
            newTable.version
        )
        newTable.isPartitioned = true
        newTable.partitionUnit = PartitionUnit.HOURS
        newTable.partitionSize = 1
        newTable.setKey()
        val setupTableStm = db!!.createStatement()
        setupTableStm.executeUpdate(
            "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
                    + "VALUES ("
                    + newTable.id
                    + ", 'area1', 'vertical1', 'name4', '1.0', TRUE, 'HOURS', 1, 1, 1, 'area1.vertical1.name4.1.0')"
        )
        setupTableStm.close()
        return newTable
    }

    /**
     * Create two dependency objects which are set to the same values as 2 already present tables in
     * the DB
     *
     * @param tableId Id from the table for which the dependencies will be set
     * @return array of dependency objects
     */
    fun createThreeDependencies(tableId: Long): Array<DependencyModel> {
        val depA = DependencyModel.builder().build()
        depA.area = "area1"
        depA.vertical = "vertical1"
        depA.tableName = "name1"
        depA.version = "1.0"
        depA.format = "parquet"
        depA.transformType = PartitionTransformType.IDENTITY
        depA.transformPartitionSize = 1
        depA.tableId = tableId
        depA.setKey()

        val depB = DependencyModel.builder().build()
        depB.area = "area1"
        depB.vertical = "vertical1"
        depB.tableName = "name2"
        depB.version = "1.0"
        depB.format = "parquet"
        depB.transformType = PartitionTransformType.WINDOW
        depB.transformPartitionSize = 2
        depB.tableId = tableId
        depB.setKey()

        val depC = DependencyModel.builder().build()
        depC.area = "area1"
        depC.vertical = "vertical1"
        depC.tableName = "name3"
        depC.version = "1.0"
        depC.format = "parquet"
        depC.transformType = PartitionTransformType.NONE
        depC.tableId = tableId
        depC.setKey()
        return arrayOf(depA, depB, depC)
    }

    fun createBadDependency(tableId: Long): Array<DependencyModel> {
        val depA = DependencyModel.builder().build()
        depA.area = "area1"
        depA.vertical = "vertical1"
        depA.tableName = "badtable"
        depA.version = "10.x"
        depA.format = "jdbc"
        depA.transformType = PartitionTransformType.IDENTITY
        depA.transformPartitionSize = 1
        depA.tableId = tableId
        depA.setKey()
        return arrayOf(depA)
    }

    @Test
    @Throws(SQLException::class)
    fun findCorrectTest() {
        val newTable = insertTestTable()
        val dependencies = createThreeDependencies(newTable.id)
        val partitionedDependency = arrayOf(dependencies[0], dependencies[1])
        newTable.dependencies = partitionedDependency
        val dao = DependencyDAO()
        val checkRes = dao.findTables(newTable.dependencies)
        Assertions.assertTrue(checkRes.missingNames!!.isEmpty())
        for (res in checkRes.foundTables!!) {
            Assertions.assertEquals(PartitionUnit.HOURS, res.partitionUnit)
            Assertions.assertEquals(1, res.partitionSize)
        }
    }

    @Test
    @Throws(SQLException::class)
    fun notFindIncorrectTest() {
        val newTable = insertTestTable()
        val dependencies = createBadDependency(newTable.id)
        newTable.dependencies = dependencies
        val dao = DependencyDAO()
        val checkRes = dao.findTables(newTable.dependencies)
        Assertions.assertTrue(checkRes.missingNames!!.contains(dependencies[0].tableName))
    }

    @Test
    @Throws(SQLException::class)
    fun addAndDeleteTest() {
        val newTable = insertTestTable()
        val dependencies = createThreeDependencies(newTable.id)
        val tableNames: Set<String> = Arrays.stream(dependencies)
            .map { obj: DependencyModel -> obj.tableName }
            .collect(Collectors.toCollection(Supplier { HashSet() }))
        newTable.dependencies = dependencies
        val dao = DependencyDAO()
        DependencyRepo().save(dependencies.toList())
        var storedDependencies = dao.getByTableId(newTable.id)
        Assertions.assertEquals(3, storedDependencies.size)
        for (d in storedDependencies) {
            Assertions.assertTrue(tableNames.contains(d.tableName))
        }
        DependencyRepo().deactivateByTableKey(newTable.id)
        storedDependencies = dao.getByTableId(newTable.id)
        Assertions.assertEquals(0, storedDependencies.size)
    }

    @Test
    @Throws(SQLException::class)
    fun testDependencyChecking() {
        val newTable = insertTestTable()
        val dependencies = createThreeDependencies(newTable.id)
        newTable.dependencies = dependencies
        val dao = DependencyDAO()
        val checkRes = dao.findTables(newTable.dependencies)
        val testRes1 = checkPartitionSizes(
            dependencies,
            newTable.partitionUnit,
            newTable.partitionSize,
            checkRes.foundTables
        )
        Assertions.assertTrue(testRes1.success)
        val tablePartitionInfo = ArrayList<TablePartRes>()
        val tableA = TablePartRes()
        tableA.tableId = dependencies[0].dependencyTableId
        tableA.partitionUnit = PartitionUnit.HOURS
        tableA.partitionSize = 1
        tablePartitionInfo.add(tableA)
        val tableB = TablePartRes()
        tableB.tableId = dependencies[1].dependencyTableId
        tableB.partitionUnit = PartitionUnit.MINUTES
        tableB.partitionSize = 15
        tablePartitionInfo.add(tableB)
        val testRes2 = checkPartitionSizes(
            dependencies,
            newTable.partitionUnit,
            newTable.partitionSize,
            tablePartitionInfo
        )
        Assertions.assertFalse(testRes2.success)
        Assertions.assertEquals(dependencies[1].tableName, testRes2.badTableNames[0])
    }

    @Test
    @Throws(SQLException::class)
    fun basicFullSaveTest() {
        val newTable = insertTestTable()
        val dependencies = createThreeDependencies(newTable.id)
        newTable.dependencies = dependencies
        val dao = DependencyDAO()
        val saveRes = dao.save(newTable, true)
        Assertions.assertTrue(saveRes.isSuccess)
        Assertions.assertFalse(saveRes.isError)
        Assertions.assertEquals(3, saveRes.dependenciesSaved.size)
        val foundDependencies = dao.getByTableId(newTable.id)
        Assertions.assertEquals(3, foundDependencies.size)
        Assertions.assertEquals("area1", foundDependencies[2].area)
        Assertions.assertEquals("vertical1", foundDependencies[2].vertical)
        Assertions.assertEquals(
            java.util.Set.of("name1", "name2", "name3"),
            foundDependencies.map { it.tableName }.toSet()
        )
        Assertions.assertEquals("1.0", foundDependencies[2].version)
        Assertions.assertEquals("parquet", foundDependencies[2].format)
        Assertions.assertEquals(
            java.util.Set.of(
                PartitionTransformType.IDENTITY,
                PartitionTransformType.WINDOW,
                PartitionTransformType.NONE
            ),
            foundDependencies.map { it.transformType }.toSet()
        )
        Assertions.assertEquals(newTable.id, foundDependencies[2].tableId)
    }

    @Test
    @Throws(SQLException::class)
    fun testStoredDependenciesAfterSaving() {
        // relies on most of the sub-methods
        val newTable = insertTestTable()
        val dependencies = createThreeDependencies(newTable.id)
        newTable.dependencies = dependencies
        val dao = DependencyDAO()
        // First use normal save method to store dependencies
        val saveRes = dao.save(newTable, true)
        Assertions.assertTrue(saveRes.isSuccess)
        val checkRes = dao.checkExistingDependencies(newTable.id, dependencies)
        // Then see if a ExistingDependencyCheck would find them and say that they are the same
        Assertions.assertTrue(checkRes.found)
        Assertions.assertTrue(checkRes.correct)
    }

    @Test
    @Throws(SQLException::class)
    fun letUnpartitionedDependonPartitioned() {
        // This should be stopped at storing
        val unpartitionedTable = TableModel.builder().build();
        unpartitionedTable.area = "area1"
        unpartitionedTable.vertical = "vertical1"
        unpartitionedTable.name = "name5"
        unpartitionedTable.version = "1.0"
        unpartitionedTable.className = java.lang.String.join(
            ".",
            unpartitionedTable.area,
            unpartitionedTable.vertical,
            unpartitionedTable.name,
            unpartitionedTable.version
        )
        unpartitionedTable.isPartitioned = false
        unpartitionedTable.setKey()
        val setupTableStm = db!!.createStatement()
        setupTableStm.executeUpdate(
            "INSERT INTO TABLE_(id, area, vertical, name, version, partitioned, parallelism, max_bulk_size, class_name)"
                    + "VALUES ("
                    + unpartitionedTable.id
                    + ", 'area1', 'vertical1', 'name5', '1.0', FALSE, 1, 1, 'area1.vertical1.name5.1.0')"
        )
        setupTableStm.close()
        val depA = DependencyModel.builder().build()
        depA.area = "area1"
        depA.vertical = "vertical1"
        depA.tableName = "name1"
        depA.version = "1.0"
        depA.format = "parquet"
        depA.transformType = PartitionTransformType.IDENTITY
        depA.transformPartitionSize = 1
        depA.tableId = unpartitionedTable.id
        depA.setKey()
        val dependencies = arrayOf(depA)
        unpartitionedTable.dependencies = dependencies
        val dao = DependencyDAO()
        // First use normal save method to store dependencies
        val saveRes = dao.save(unpartitionedTable, true)
        Assertions.assertFalse(saveRes.isSuccess)
        depA.transformType = PartitionTransformType.NONE
        val saveRes2 = dao.save(unpartitionedTable, true)
        Assertions.assertFalse(saveRes2.isSuccess)
    }

    @Test
    fun checkKeyGeneration() {
        val tableDepA = TableModel.builder().build();
        tableDepA.area = "area1"
        tableDepA.vertical = "vertical1"
        tableDepA.name = "name1"
        tableDepA.version = "1.0"
        tableDepA.setKey()
        val depATarget = Target()
        depATarget.tableId = tableDepA.id
        depATarget.format = "parquet"
        depATarget.setKey()
        val tableB = TableModel.builder().build();
        tableB.area = "area1"
        tableB.vertical = "vertical1"
        tableB.name = "name2"
        tableB.version = "1.0"
        tableB.setKey()
        val depA = DependencyModel.builder().build()
        depA.area = "area1"
        depA.vertical = "vertical1"
        depA.tableName = "name1"
        depA.version = "1.0"
        depA.format = "parquet"
        depA.tableId = tableB.id
        depA.setKey()
        Assertions.assertEquals(depATarget.id, depA.dependencyTargetId)
    }

    @Test
    fun compareDependencies() {
        val table = TableModel.builder().build();
        table.area = "area1"
        table.vertical = "vertical1"
        table.name = "name2"
        table.version = "1.0"
        table.setKey()
        val depA = DependencyModel.builder().build()
        depA.area = "area1"
        depA.vertical = "vertical1"
        depA.tableName = "name1"
        depA.version = "1.0"
        depA.format = "parquet"
        depA.transformType = PartitionTransformType.IDENTITY
        depA.transformPartitionSize = 1
        depA.tableId = table.id
        depA.setKey()
        val depB = DependencyModel.builder().build()
        depB.area = "area1"
        depB.vertical = "vertical1"
        depB.tableName = "name1"
        depB.version = "1.0"
        depB.format = "parquet"
        depB.transformType = PartitionTransformType.IDENTITY
        depB.transformPartitionSize = 1
        depB.tableId = table.id
        depB.setKey()
        val res1 = compareDependency(depA, depB)
        Assertions.assertTrue(res1.success)
        depA.transformType = PartitionTransformType.WINDOW
        depA.transformPartitionSize = 2
        val res2 = compareDependency(depA, depB)
        Assertions.assertFalse(res2.success)
        Assertions.assertTrue(res2.problem!!.contains("type"))
    }
}