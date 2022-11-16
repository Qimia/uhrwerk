import io.qimia.uhrwerk.ConnectionHelper
import io.qimia.uhrwerk.common.metastore.model.*
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.common.model.TargetModel
import org.junit.jupiter.api.*
import java.sql.*
import java.sql.Connection
import java.time.LocalDateTime

class ProcessPartitionTest {

    lateinit var db: Connection
    private val insertPartitionQuery = "INSERT INTO PARTITION_(id, target_id, partition_ts, partitioned) VALUES(?,?,?,?)"

    lateinit var connFS: ConnectionModel
    var tableDepA: TableModel? = null
    var tableDepB: TableModel? = null
    var depATarget: TargetModel? = null
    var depBTarget: TargetModel? = null
    lateinit var filledPartitionsA: Array<LocalDateTime>
    lateinit var partitionsA: Array<Partition?>

    @BeforeEach
    @Throws(SQLException::class)
    fun setUp() {
        db = ConnectionHelper.getConnecion()
    }

    @AfterEach
    @Throws(SQLException::class)
    fun tearDown() {
        // WARNING deletes all data as cleanup
        val deletePartDependencies = db!!.createStatement() // In case of some lost source data
        deletePartDependencies.execute("DELETE FROM PARTITION_DEPENDENCY")
        deletePartDependencies.close()
        val deletePartitions = db!!.createStatement()
        deletePartitions.execute("DELETE FROM PARTITION_")
        deletePartitions.close()
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
        if (db != null) if (!db!!.isClosed) db!!.close()
    }

    @Throws(SQLException::class)
    fun setupTableA() {
        // Setup 2 tables to depend on with targets (which have a connection)
        connFS = generateConnection()

        val a = db!!.createStatement()
        a.executeUpdate(
                "INSERT INTO CONNECTION (id, name, type, path) VALUES ("
                        + connFS.id
                        + ", 'a_connection', 'FS', '/some/test/path')")
        a.close()
        tableDepA = TableModel.builder().build()
        tableDepA!!.area = "area1"
        tableDepA!!.vertical = "vertical1"
        tableDepA!!.name = "name1"
        tableDepA!!.version = "1.0"
        tableDepA!!.isPartitioned = true
        tableDepA!!.partitionSize = 1
        tableDepA!!.partitionUnit = PartitionUnit.HOURS
        tableDepA!!.setKey()
        val b = db!!.createStatement()
        b.executeUpdate(
                "INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
                        + "VALUES ("
                        + tableDepA!!.id
                        + ", 'area1', 'vertical1', 'name1', '1.0', 'HOURS', 1, 1, 1, 'area1.vertical1.name1.1.0')")
        b.close()
        depATarget = Target()
        depATarget!!.tableId = tableDepA!!.id
        depATarget!!.format = "parquet"
        depATarget!!.setKey()
        val c = db!!.createStatement()
        c.executeUpdate(
                "INSERT INTO TARGET (id, table_id, connection_id, format)"
                        + "VALUES ("
                        + depATarget!!.id
                        + ","
                        + tableDepA!!.id
                        + ","
                        + connFS.id
                        + ", 'parquet')")
        c.close()
        filledPartitionsA = arrayOf(
                LocalDateTime.of(2020, 4, 10, 10, 0),
                LocalDateTime.of(2020, 4, 10, 11, 0),
                LocalDateTime.of(2020, 4, 10, 12, 0)
        )
        partitionsA = arrayOfNulls(filledPartitionsA.size)
        val insert = db!!.prepareStatement(insertPartitionQuery)
        for (i in partitionsA.indices) {
            val p = Partition.builder().build()
            p.targetId = depATarget!!.id
            p.isPartitioned = true
            p.partitionTs = filledPartitionsA[i]
            p.partitionUnit = PartitionUnit.HOURS
            p.partitionSize = 1
            p.setKey()
            partitionsA[i] = p
            insert.setLong(1, p.id)
            insert.setLong(2, p.targetId)
            insert.setTimestamp(3, Timestamp.valueOf(p.partitionTs))
            insert.setBoolean(4, p.isPartitioned)
            insert.addBatch()
        }
        insert.executeBatch()
        insert.close()
    }

    @Test
    @Throws(SQLException::class)
    fun checkSingleIdentityDependency() {
        setupTableA()

        // Setup a simple one on one data model
        val tableOut = TableModel.builder().build()
        tableOut.area = "area1"
        tableOut.vertical = "vertical1"
        tableOut.name = "tableout"
        tableOut.version = "1.0"
        tableOut.isPartitioned = true
        tableOut.partitionSize = 1
        tableOut.partitionUnit = PartitionUnit.HOURS
        tableOut.setKey()

        val dependencyIn = DependencyModel.builder().build()
        dependencyIn.tableName = tableDepA!!.name
        dependencyIn.area = tableDepA!!.area
        dependencyIn.format = depATarget!!.format
        dependencyIn.vertical = tableDepA!!.vertical
        dependencyIn.version = tableDepA!!.version
        dependencyIn.dependencyTableId = tableDepA!!.id
        dependencyIn.dependencyTargetId = depATarget!!.id
        dependencyIn.tableId = tableOut.id
        dependencyIn.transformType = PartitionTransformType.IDENTITY
        dependencyIn.setKey()

        tableOut.dependencies = arrayOf(dependencyIn)

        val targetOut = Target()
        targetOut.format = "csv"
        targetOut.tableId = tableOut.id
        targetOut.connection = connFS
        targetOut.setKey()

        tableOut.targets = arrayOf(targetOut)

        val tableStm = db!!.createStatement()
        tableStm.executeUpdate(
                "INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
                        + "VALUES ("
                        + tableOut.id
                        + ", 'area1', 'vertical1', 'tableout', '1.0', 'HOURS', 1, 1, 1, 'area1.vertical1.tableout.1.0')")
        tableStm.close()

        val depStm = db!!.prepareStatement("INSERT INTO DEPENDENCY(id, table_id, dependency_target_id, dependency_table_id, "
                + "transform_type, transform_partition_size) VALUES (?, ?, ?, ?, 'IDENTITY', 1)")

        depStm.setLong(1, dependencyIn.id)
        depStm.setLong(2, dependencyIn.tableId)
        depStm.setLong(3, dependencyIn.dependencyTargetId)
        depStm.setLong(4, dependencyIn.dependencyTableId)
        depStm.executeUpdate()

        val tarStm = db!!.createStatement()
        tarStm.executeUpdate(
                "INSERT INTO TARGET (id, table_id, connection_id, format)"
                        + "VALUES ("
                        + targetOut.id
                        + ","
                        + tableOut.id
                        + ","
                        + connFS.id
                        + ", 'csv')")
        tarStm.close()

        // Already write a single Partition for the output
        val outPartition = Partition.builder().build()
        outPartition.targetId = targetOut.id
        outPartition.isPartitioned = true
        outPartition.partitionTs = filledPartitionsA[0] // The first one is already there
        outPartition.partitionUnit = PartitionUnit.HOURS
        outPartition.partitionSize = 1
        outPartition.setKey()
        val partStm = db!!.prepareStatement(insertPartitionQuery)

        partStm.setLong(1, outPartition.id)
        partStm.setLong(2, outPartition.targetId)
        partStm.setTimestamp(3, Timestamp.valueOf(outPartition.partitionTs))
        partStm.setBoolean(4, outPartition.isPartitioned)
        partStm.executeUpdate()

        val partDepStm = db!!.prepareStatement("INSERT INTO PARTITION_DEPENDENCY (id, partition_id, dependency_partition_id) VALUES (?, ?, ?)")

        val partDepId = PartitionDependencyHash.generateId(outPartition.id, partitionsA[0]!!.id)
        partDepStm.setLong(1, partDepId)
        partDepStm.setLong(2, outPartition.id)
        partDepStm.setLong(3, partitionsA[0]!!.id)
        partDepStm.executeUpdate()

        // Now call processingPartitions
        val dao = TableDAO()
        val testTimes = listOf(
                filledPartitionsA[0],  // Should be already filled
                filledPartitionsA[1],  // ready to process
                filledPartitionsA[2],
                LocalDateTime.of(2020, 4, 10, 13, 0) // Not ready yet
        )
        val resultSet = dao.processingPartitions(tableOut, testTimes)

        Assertions.assertEquals(1, resultSet.failedTs.size)
        Assertions.assertEquals(1, resultSet.processedTs.size)
        Assertions.assertEquals(2, resultSet.resolvedTs.size)
    }

    @Test
    @Throws(SQLException::class)
    fun checkSingleWindowedDependency() {
        setupTableA()

        // Setup a simple one on one data model
        val tableOut = TableModel.builder().build()
        tableOut.area = "area1"
        tableOut.vertical = "vertical1"
        tableOut.name = "tableout"
        tableOut.version = "1.0"
        tableOut.isPartitioned = true
        tableOut.partitionSize = 1
        tableOut.partitionUnit = PartitionUnit.HOURS
        tableOut.setKey()
        val dependencyIn = DependencyModel.builder().build()
        dependencyIn.tableName = tableDepA!!.name
        dependencyIn.area = tableDepA!!.area
        dependencyIn.format = depATarget!!.format
        dependencyIn.vertical = tableDepA!!.vertical
        dependencyIn.version = tableDepA!!.version
        dependencyIn.dependencyTableId = tableDepA!!.id
        dependencyIn.dependencyTargetId = depATarget!!.id
        dependencyIn.tableId = tableOut.id
        dependencyIn.transformType = PartitionTransformType.WINDOW
        dependencyIn.transformPartitionSize = 2
        dependencyIn.setKey()
        tableOut.dependencies = arrayOf(dependencyIn)
        val targetOut = Target()
        targetOut.format = "csv"
        targetOut.tableId = tableOut.id
        targetOut.connection = connFS
        targetOut.setKey()
        tableOut.targets = arrayOf(targetOut)
        val tableStm = db!!.createStatement()
        tableStm.executeUpdate(
                "INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
                        + "VALUES ("
                        + tableOut.id
                        + ", 'area1', 'vertical1', 'tableout', '1.0', 'HOURS', 1, 1, 1, 'area1.vertical1.tableout.1.0')")
        tableStm.close()
        val depStm = db!!.prepareStatement("INSERT INTO DEPENDENCY(id, table_id, dependency_target_id, dependency_table_id, "
                + "transform_type, transform_partition_size) VALUES (?, ?, ?, ?, 'WINDOW', 2)")
        depStm.setLong(1, dependencyIn.id)
        depStm.setLong(2, dependencyIn.tableId)
        depStm.setLong(3, dependencyIn.dependencyTargetId)
        depStm.setLong(4, dependencyIn.dependencyTableId)
        depStm.executeUpdate()
        val tarStm = db!!.createStatement()
        tarStm.executeUpdate(
                "INSERT INTO TARGET (id, table_id, connection_id, format)"
                        + "VALUES ("
                        + targetOut.id
                        + ","
                        + tableOut.id
                        + ","
                        + connFS.id
                        + ", 'csv')")
        tarStm.close()

        // Already write a single Partition for the output
        val outPartition = Partition.builder().build()
        outPartition.targetId = targetOut.id
        outPartition.isPartitioned = true
        outPartition.partitionTs = filledPartitionsA[1] // The second one is already there
        outPartition.partitionUnit = PartitionUnit.HOURS
        outPartition.partitionSize = 1
        outPartition.setKey()
        val partStm = db!!.prepareStatement(insertPartitionQuery)
        partStm.setLong(1, outPartition.id)
        partStm.setLong(2, outPartition.targetId)
        partStm.setTimestamp(3, Timestamp.valueOf(outPartition.partitionTs))
        partStm.setBoolean(4, outPartition.isPartitioned)
        partStm.executeUpdate()

        // Now make sure that the 2nd partition depends on the 2 previous partitions
        val partDepStm = db!!.prepareStatement("""
    INSERT INTO PARTITION_DEPENDENCY (id, partition_id, dependency_partition_id)
    VALUES (?, ?, ?);
    """.trimIndent())
        var partDepId = PartitionDependencyHash.generateId(outPartition.id, partitionsA[0]!!.id)
        partDepStm.setLong(1, partDepId)
        partDepStm.setLong(2, outPartition.id)
        partDepStm.setLong(3, partitionsA[0]!!.id)
        partDepStm.executeUpdate()
        partDepId = PartitionDependencyHash.generateId(outPartition.id, partitionsA[1]!!.id)
        partDepStm.setLong(1, partDepId)
        partDepStm.setLong(2, outPartition.id)
        partDepStm.setLong(3, partitionsA[1]!!.id)
        partDepStm.executeUpdate()

        // Now call processingPartitions
        val dao = TableDAO()
        val testTimes = listOf(
                filledPartitionsA[0],  // Can't run because 9 o clock isn't there
                filledPartitionsA[1],  // has already been filled
                filledPartitionsA[2],  // is ready to run
                LocalDateTime.of(2020, 4, 10, 13, 0) // Can't run because 13 isnt there
        )
        val resultSet = dao.processingPartitions(tableOut, testTimes)
        Assertions.assertEquals(2, resultSet.failedTs.size)
        Assertions.assertEquals(testTimes[1], resultSet.processedTs[0])
        Assertions.assertEquals(testTimes[2], resultSet.resolvedTs[0])
    }

    @Test
    @Throws(SQLException::class)
    fun checkSingleAggregatedDependency() {
        setupTableA()

        // Setup a simple one on one data model
        val tableOut = TableModel.builder().build()
        tableOut.area = "area1"
        tableOut.vertical = "vertical1"
        tableOut.name = "tableout"
        tableOut.version = "1.0"
        tableOut.isPartitioned = true
        tableOut.partitionSize = 2
        tableOut.partitionUnit = PartitionUnit.HOURS
        tableOut.setKey()
        val dependencyIn = DependencyModel.builder().build()
        dependencyIn.tableName = tableDepA!!.name
        dependencyIn.area = tableDepA!!.area
        dependencyIn.format = depATarget!!.format
        dependencyIn.vertical = tableDepA!!.vertical
        dependencyIn.version = tableDepA!!.version
        dependencyIn.dependencyTableId = tableDepA!!.id
        dependencyIn.dependencyTargetId = depATarget!!.id
        dependencyIn.tableId = tableOut.id
        dependencyIn.transformType = PartitionTransformType.AGGREGATE
        dependencyIn.transformPartitionSize = 2
        dependencyIn.setKey()
        tableOut.dependencies = arrayOf(dependencyIn)
        val targetOut = Target()
        targetOut.format = "csv"
        targetOut.tableId = tableOut.id
        targetOut.connection = connFS
        targetOut.setKey()
        tableOut.targets = arrayOf(targetOut)
        val tableStm = db!!.createStatement()
        tableStm.executeUpdate(
                "INSERT INTO TABLE_(id, area, vertical, name, version, partition_unit, partition_size, parallelism, max_bulk_size, class_name)"
                        + "VALUES ("
                        + tableOut.id
                        + ", 'area1', 'vertical1', 'tableout', '1.0', 'HOURS', 2, 1, 1, 'area1.vertical1.tableout.1.0')")
        tableStm.close()
        val depStm = db!!.prepareStatement("INSERT INTO DEPENDENCY(id, table_id, dependency_target_id, dependency_table_id, "
                + "transform_type, transform_partition_size) VALUES (?, ?, ?, ?, 'AGGREGATE', 2)")
        depStm.setLong(1, dependencyIn.id)
        depStm.setLong(2, dependencyIn.tableId)
        depStm.setLong(3, dependencyIn.dependencyTargetId)
        depStm.setLong(4, dependencyIn.dependencyTableId)
        depStm.executeUpdate()
        val tarStm = db!!.createStatement()
        tarStm.executeUpdate(
                "INSERT INTO TARGET (id, table_id, connection_id, format)"
                        + "VALUES ("
                        + targetOut.id
                        + ","
                        + tableOut.id
                        + ","
                        + connFS.id
                        + ", 'csv')")
        tarStm.close()

        // First test without having any partitions written (with 2 hour batches ofcourse)
        val dao = TableDAO()
        val testTimes = listOf(
                filledPartitionsA[0],  // should be ready to run
                filledPartitionsA[2])
        var resultSet = dao.processingPartitions(tableOut, testTimes)
        Assertions.assertEquals(testTimes[1], resultSet.failedTs[0])
        Assertions.assertEquals(0, resultSet.processedTs.size)
        Assertions.assertEquals(testTimes[0], resultSet.resolvedTs[0])

        // Now we fill the first testTimes partition for the outtable
        val outPartition = Partition.builder().build()
        outPartition.targetId = targetOut.id
        outPartition.isPartitioned = true
        outPartition.partitionTs = filledPartitionsA[0] // The second one is already there
        outPartition.partitionUnit = PartitionUnit.HOURS
        outPartition.partitionSize = 2
        outPartition.setKey()
        val partStm = db!!.prepareStatement(insertPartitionQuery)
        partStm.setLong(1, outPartition.id)
        partStm.setLong(2, outPartition.targetId)
        partStm.setTimestamp(3, Timestamp.valueOf(outPartition.partitionTs))
        partStm.setBoolean(4, outPartition.isPartitioned)
        partStm.executeUpdate()

        // Now make sure that the 1st partition depends on the 2 partitions of the dependency
        val partDepStm = db!!.prepareStatement("""
    INSERT INTO PARTITION_DEPENDENCY (id, partition_id, dependency_partition_id)
    VALUES (?, ?, ?);
    """.trimIndent())
        var partDepId = PartitionDependencyHash.generateId(outPartition.id, partitionsA[0]!!.id)
        partDepStm.setLong(1, partDepId)
        partDepStm.setLong(2, outPartition.id)
        partDepStm.setLong(3, partitionsA[0]!!.id)
        partDepStm.executeUpdate()
        partDepId = PartitionDependencyHash.generateId(outPartition.id, partitionsA[1]!!.id)
        partDepStm.setLong(1, partDepId)
        partDepStm.setLong(2, outPartition.id)
        partDepStm.setLong(3, partitionsA[1]!!.id)
        partDepStm.executeUpdate()

        // Now we do the same processingPartitions call as before and we should see a processedTs
        resultSet = dao.processingPartitions(tableOut, testTimes)
        Assertions.assertEquals(testTimes[1], resultSet.failedTs[0])
        Assertions.assertEquals(testTimes[0], resultSet.processedTs[0])
        Assertions.assertEquals(0, resultSet.resolvedTs.size)
    }

    @Throws(SQLException::class)
    fun setupTableB() {
        // Setup an unpartitioned Table
        connFS = generateConnection()

        val a = db!!.createStatement()
        a.executeUpdate(
                "INSERT INTO CONNECTION (id, name, type, path) VALUES ("
                        + connFS.id
                        + ", 'a_connection', 'FS', '/some/test/path')")
        a.close()

        tableDepB = TableModel.builder().build()
        tableDepB!!.area = "area1"
        tableDepB!!.vertical = "vertical1"
        tableDepB!!.name = "name2"
        tableDepB!!.version = "1.0"
        tableDepB!!.isPartitioned = false
        tableDepB!!.setKey()
        val b = db!!.createStatement()
        b.executeUpdate(
                "INSERT INTO TABLE_(id, area, vertical, name, version, parallelism, max_bulk_size, class_name)"
                        + "VALUES ("
                        + tableDepB!!.id
                        + ", 'area1', 'vertical1', 'name1', '1.0', 1, 1, 'area1.vertical1.name1.1.0')")
        b.close()

        depBTarget = Target()
        depBTarget!!.tableId = tableDepB!!.id
        depBTarget!!.format = "parquet"
        depBTarget!!.setKey()
        val c = db!!.createStatement()
        c.executeUpdate(
                "INSERT INTO TARGET (id, table_id, connection_id, format)"
                        + "VALUES ("
                        + depBTarget!!.id
                        + ","
                        + depBTarget!!.id
                        + ","
                        + connFS.id
                        + ", 'parquet')")
        c.close()
        // Insert will be processed later
    }

    private fun generateConnection(): ConnectionModel {
        val conn = ConnectionModel.builder()
                .name("a_connection")
                .type(ConnectionType.FS)
                .build()
        return conn
    }


}