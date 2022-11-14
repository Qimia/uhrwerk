import io.qimia.uhrwerk.ConnectionHelper
import io.qimia.uhrwerk.common.metastore.config.SourceResult
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.common.model.TargetModel
import io.qimia.uhrwerk.config.ConnectionBuilder
import io.qimia.uhrwerk.config.DependencyBuilder
import io.qimia.uhrwerk.config.SourceBuilder
import io.qimia.uhrwerk.config.TableBuilder
import io.qimia.uhrwerk.dao.JdbcBackendUtils.getPartitionTs
import io.qimia.uhrwerk.TestData
import org.apache.log4j.Logger
import org.junit.jupiter.api.*
import java.sql.SQLException
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*

internal class TableDAOTest {
    private val logger = Logger.getLogger(this.javaClass)
    var db: java.sql.Connection? = null
    var tableDAO: TableDAO? = null
    var connectionDAO: ConnectionDAO? = null
    var partitionDAO: PartitionDAO? = null
    fun generateSources(tableId: Long): Array<SourceModel?> {
        val sources = arrayOfNulls<SourceModel>(2)
        for (i in sources.indices) {
            sources[i] = TestData.source("dummy", tableId, 100)
            sources[i]!!.path = "path$i"
            sources[i]!!.tableId = tableId
        }
        return sources
    }

    private fun generateTable(): TableModel {
        val table = TableModel.builder().build()
        table.area = "dwh"
        table.vertical = "vertical1"
        table.name = "tableA"
        table.className = listOf(
            table.area,
            table.vertical,
            table.name,
            table.version
        ).joinToString(separator = ".")
        table.partitionUnit = PartitionUnit.MINUTES
        table.partitionSize = 15
        table.parallelism = 8
        table.maxBulkSize = 96
        table.version = "1.0"
        table.isPartitioned = true
        table.setKey()
        table.sources = generateSources(table.id)
        table.dependencies = generateDependencies(table.id)
        table.targets = generateTargets(table.id)
        return table
    }

    fun generateDependencies(tableId: Long): Array<DependencyModel?> {
        val dependencies = arrayOfNulls<DependencyModel>(2)
        for (i in dependencies.indices) {
            val table = TableModel.builder().build()
            table.area = "staging"
            table.vertical = "vertical2"
            table.name = "tableDep$i"
            table.partitionUnit = PartitionUnit.MINUTES
            table.partitionSize = 15
            table.parallelism = 8
            table.maxBulkSize = 96
            table.version = "1.0"
            table.className = listOf(
                table.area,
                table.vertical,
                table.name,
                table.version
            ).joinToString(separator = ".")
            table.setKey()
            tableDAO!!.save(table, false)
            val target = generateTarget(table.id, i)
            TargetDAO().save(listOf(target), target.tableId, false)
            dependencies[i] = DependencyModel.builder().build()
            dependencies[i]!!.transformType = PartitionTransformType.IDENTITY
            dependencies[i]!!.transformPartitionUnit = PartitionUnit.MINUTES
            dependencies[i]!!.transformPartitionSize = 15
            dependencies[i]!!.format = target.format
            dependencies[i]!!.tableName = table.name
            dependencies[i]!!.area = table.area
            dependencies[i]!!.vertical = table.vertical
            dependencies[i]!!.version = table.version
            dependencies[i]!!.tableId = tableId
            dependencies[i]!!.setKey()
        }
        return dependencies
    }

    fun generateTarget(tableId: Long, i: Int): TargetModel {
        val target = Target()
        val conn: ConnectionModel = TestData.connection("dummy")
        ConnectionDAO().save(conn, true)
        target.format = "csv$i"
        target.tableId = tableId
        target.connection = conn
        target.setKey()
        return target
    }

    fun generateTargets(tableId: Long): Array<TargetModel?> {
        val targets = arrayOfNulls<TargetModel>(2)
        for (i in targets.indices) {
            targets[i] = generateTarget(tableId, i)
            targets[i]!!.setKey()
        }
        return targets
    }

    @BeforeEach
    @Throws(SQLException::class)
    fun setUp() {
        db = ConnectionHelper.getConnecion()
        tableDAO = TableDAO()
        connectionDAO = ConnectionDAO()
        partitionDAO = PartitionDAO()

        // clean
        db!!.prepareStatement("delete from TABLE_").execute()
        db!!.prepareStatement("delete from TARGET").execute()
        db!!.prepareStatement("delete from SOURCE").execute()
        db!!.prepareStatement("delete from DEPENDENCY").execute()
        db!!.prepareStatement("delete from CONNECTION").execute()
        db!!.prepareStatement("delete from PARTITION_").execute()
    }

    @AfterEach
    @Throws(SQLException::class)
    fun tearDown() {
        if (db != null) if (!db!!.isClosed) db!!.close()
    }

    @Test
    fun saveTable() {
        val table: TableModel = TestData.table("dummy")
        table.isPartitioned = false
        val tableResult = tableDAO!!.save(table, true)
        Assertions.assertTrue(tableResult.isSuccess)
        Assertions.assertFalse(tableResult.isError)
        Assertions.assertEquals(table, tableResult.newResult)
    }

    @Test
    fun saveTableWithAllChildren() {
        val table = generateTable()
        val result = tableDAO!!.save(table, false)
        logger.info(result.message)
        Assertions.assertTrue(result.isSuccess)
        Arrays.stream(result.sourceResults)
            .forEach { sourceResult: SourceResult -> Assertions.assertTrue(sourceResult.isSuccess) }
        Assertions.assertTrue(result.targetResult.isSuccess)
        Assertions.assertTrue(result.dependencyResult.isSuccess)

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
        val connectionId = table.sources[0].connection.id
        val Connection = ConnectionModel.builder()
            .id(connectionId)
            .build()
        table.sources[0].connection = Connection
        val targetConnectionId = table.targets[0].connection.id
        val ConnectionTarget = ConnectionModel.builder()
            .id(targetConnectionId)
            .build()
        table.targets[0].connection = ConnectionTarget
        val result2 = tableDAO!!.save(table, false)
        logger.info(result2.message)
        Assertions.assertTrue(result2.isSuccess)
        Arrays.stream(result2.sourceResults)
            .forEach { sourceResult: SourceResult -> Assertions.assertTrue(sourceResult.isSuccess) }
        Assertions.assertTrue(result2.targetResult.isSuccess)
        Assertions.assertTrue(result2.dependencyResult.isSuccess)
    }

    @Test
    fun savingChangedTableTwiceShouldFail() {
        val table = generateTable()
        var result = tableDAO!!.save(table, false)
        Assertions.assertTrue(result.isSuccess)
        table.isPartitioned = false
        result = tableDAO!!.save(table, false)
        logger.info(result.message)
        Assertions.assertFalse(result.isSuccess)
    }

    @Test
    fun savingSameTableTwiceShouldSucceed() {
        val table = generateTable()
        var result = tableDAO!!.save(table, false)
        Assertions.assertTrue(result.isSuccess)
        result = tableDAO!!.save(table, false)
        logger.info(result.message)
        Assertions.assertTrue(result.isSuccess)
    }

    @Test
    fun savingChangedTableTwiceShouldSucceedWithOverwrite() {
        val table = generateTable()
        var result = tableDAO!!.save(table, false)
        Assertions.assertTrue(result.isSuccess)
        table.parallelism = 9874263
        result = tableDAO!!.save(table, true)
        Assertions.assertTrue(result.isSuccess)
    }

    @Test
    @Throws(SQLException::class)
    fun tableBuilderTest() {
        val connections = listOf(
            ConnectionBuilder()
                .name("Test-JDBC-Source1")
                .jdbc()
                .jdbcUrl("url")
                .jdbcDriver("driver")
                .user("user")
                .pass("pass")
                .done()
                .build(),
            ConnectionBuilder()
                .name("S3")
                .s3()
                .path("S3Path")
                .secretId("ID")
                .secretKey("key")
                .done()
                .build()
        )
        val connResults = connections.map { connectionDAO!!.save(it, true) }
        connResults.forEach {
            Assertions.assertTrue(it.isSuccess)
            Assertions.assertNotNull(it.newConnection)
            Assertions.assertNull(it.oldConnection)
        }
        val source1 = SourceBuilder()
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
            .build()
        val depTable1 = TableBuilder()
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
            .build()
        val depTable1Result = tableDAO!!.save(depTable1, true)
        Assertions.assertFalse(depTable1Result.isError)
        Assertions.assertTrue(depTable1Result.isSuccess)
        Assertions.assertNotNull(depTable1Result.newResult)
        Assertions.assertNull(depTable1Result.oldResult)
        Assertions.assertNotNull(depTable1.id)
        Assertions.assertNotNull(depTable1.targets[0].id)
        val dep1 = DependencyBuilder()
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
            .build()
        val mainTable = TableBuilder()
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
            .build()
        val mainTableResult = tableDAO!!.save(mainTable, true)
        Assertions.assertFalse(mainTableResult.isError)
        Assertions.assertTrue(mainTableResult.isSuccess)
        Assertions.assertNotNull(mainTableResult.newResult)
        Assertions.assertNull(mainTableResult.oldResult)
        val start = LocalDateTime.of(2020, 8, 24, 9, 0)
        val end = LocalDateTime.of(2020, 8, 24, 17, 0)
        val duration1 = Duration.of(30, ChronoUnit.MINUTES)
        val partitionTs = getPartitionTs(start, end, duration1)
        for (ts in partitionTs) {
            val partition = Partition.builder().build()
            partition.targetId = depTable1.targets[0].id
            partition.partitionTs = ts
            partition.setKey()
            val partitionResult = partitionDAO!!.save(partition, true)
        }
        val duration2 = Duration.of(1, ChronoUnit.HOURS)
        val requestedTs = getPartitionTs(start, end, duration2)
        val partitionResultSet = tableDAO!!.processingPartitions(mainTable, requestedTs)
        for (i in partitionResultSet.resolvedTs.indices) {
            val partitionResult = partitionResultSet.resolved[i]
            logger.info(partitionResultSet.resolvedTs[i])
            val resolvedDependency = partitionResult.resolvedDependencies[0]
            for (j in resolvedDependency.succeeded.indices) {
                logger.info(resolvedDependency.succeeded[j])
            }
        }
    }

    @Test
    fun savingPartitionedSourceForUnpartitionedTableShouldFail() {
        val table = generateTable()
        table.isPartitioned = false
        val result = tableDAO!!.save(table, false)
        Assertions.assertFalse(result.isSuccess)
    }
}