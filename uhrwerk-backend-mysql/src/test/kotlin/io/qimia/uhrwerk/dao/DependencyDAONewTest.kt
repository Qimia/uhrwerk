package io.qimia.uhrwerk.dao

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import io.qimia.uhrwerk.common.metastore.builders.DependencyModelBuilder
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.repo.*
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.SQLException
import java.util.*

@Testcontainers
class DependencyDAONewTest {
    val dao = DependencyDAO()

    private val tableNames = listOf("TableA", "TableB", "TableC")

    private var tables: List<TableModel>? = null

    private var connection: ConnectionModel? = null

    private var targets: List<TargetModel>? = null


    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("DEPENDENCY", LOGGER)
        TestUtils.cleanData("TARGET", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun addDeps() {
        val tableNames = listOf("TableA", "TableB", "TableC")
        tables = tableNames.map { TestData.table(it) }
        tables!!.forEach {
            TableRepo().save(it)

        }

        connection =
            ConnectionRepo().save(TestData.connection("Connection-DependencyDAOTest"))

        targets = tables!!.map { TestData.target(it.id!!, connection!!.id!!) }

        targets = TargetRepo().save(targets!!)
    }

    @Test
    @Throws(SQLException::class)
    fun notFindIncorrectTest() {
        val depA = DependencyModelBuilder()
            .area("area1")
            .vertical("vertical1")
            .tableName("badtable")
            .version("10.x")
            .format("jdbc")
            .transformType(PartitionTransformType.IDENTITY)
            .transformPartitionSize(1)
            .tableId(tables!![0].id!!).build()

        val checkRes = dao.findTables(arrayOf(depA))
        Truth.assertThat(checkRes.missingNames!!).contains(depA.tableName)
    }

    @Test
    @Throws(SQLException::class)
    fun addAndDeleteTest() {
        val table = tables!![0]


        val dep1 =
            TestData.dependency(table, tables!![1], "parquet")

        val dep2 = TestData.dependency(
            table,
            tables!![2],
            "parquet",
            trfType = PartitionTransformType.WINDOW,
            trfPartSize = 2
        )

        val dependencies = listOf(dep1, dep2)


        table.dependencies = dependencies.toTypedArray()

        val trParams = listOf(tables!![1], tables!![2]).map { Pair(it.id, "parquet") }

        val targets1 = trParams.flatMap { TargetRepo().getByTableIdFormat(it.first!!, it.second) }

        val result = dao.save(table, false)
        LOGGER.error(result.toString())
        Truth.assertThat(result.isSuccess).isTrue()
        Truth.assertThat(result.isError).isFalse()

        var storedDependencies = dao.getByTableId(table.id!!)
        Assertions.assertEquals(2, storedDependencies.size)
        for (d in storedDependencies) {
            Assertions.assertTrue(tableNames.contains(d.tableName))
        }
        DependencyRepo().deactivateByTableId(table.id!!)
        storedDependencies = dao.getByTableId(table.id!!)
        Assertions.assertEquals(0, storedDependencies.size)
    }


    companion object {

        private val LOGGER = LoggerFactory.getLogger(DependencyDAONewTest::class.java)

        @Container
        var MY_SQL_DB: MySQLContainer<*> = TestUtils.mysqlContainer()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val logConsumer = Slf4jLogConsumer(LOGGER)
            MY_SQL_DB.followOutput(logConsumer)
            HikariCPDataSource.initConfig(
                MY_SQL_DB.jdbcUrl,
                MY_SQL_DB.username,
                MY_SQL_DB.password
            )
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            HikariCPDataSource.close()
        }
    }
}