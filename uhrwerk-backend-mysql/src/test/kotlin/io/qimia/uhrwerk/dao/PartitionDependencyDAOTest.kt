package io.qimia.uhrwerk.dao

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import io.qimia.uhrwerk.TestHelper
import io.qimia.uhrwerk.common.metastore.config.PartitionDependencyService
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult
import io.qimia.uhrwerk.common.model.ConnectionModel
import io.qimia.uhrwerk.common.model.Partition
import io.qimia.uhrwerk.common.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import io.qimia.uhrwerk.repo.*
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.LocalDateTime

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
internal class PartitionDependencyDAOTest {

    private val service: PartitionDependencyService = PartitionDependencyDAO()

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var target: TargetModel? = null

    private var timestamps: List<LocalDateTime>? = null
    private var partitions: List<Partition>? = null


    @AfterEach
    fun cleanUp() {
        TestHelper.cleanData("PARTITION_DEPENDENCY", LOGGER)
        TestHelper.cleanData("PARTITION_", LOGGER)
        TestHelper.cleanData("TARGET", LOGGER)
        TestHelper.cleanData("TABLE_", LOGGER)
        TestHelper.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
        connection = ConnectionRepo().save(TestData.connection("Connection-PartitionTest"))
        table = TableRepo().save(TestData.table("Table-PartitionTest"))
        target = TargetRepo().save(TestData.target(connection!!.id, table!!.id))

        timestamps = TestData.timestamps(15)

        val parts = timestamps!!.map {
            TestData.partition(
                targetId = target!!.id,
                partitionTs = it
            )
        }
        val partRes = PartitionDAO().save(parts, true)
        partitions = partRes.map { it.newResult }
    }

    @Test
    @Order(1)
    fun save() {
        val batchTs = timestamps!!.last()
        val childPart = partitions!!.first()
        val parentParts = partitions!!.slice(0 until partitions!!.size - 1)

        val dependencyResult = DependencyResult()
        dependencyResult.partitionTs = batchTs
        dependencyResult.partitions = parentParts.toTypedArray()

        val result = service.saveAll(childPart.id, arrayOf(dependencyResult), false)

        Truth.assertThat(result.isSuccess).isTrue()
        Truth.assertThat(result.isError).isFalse()
    }

    @Test
    @Order(2)
    fun saveOverwrite() {

        val batchTs = timestamps!!.last()
        val childPart = partitions!!.first()
        val parentParts = partitions!!.slice(0 until partitions!!.size - 1)

        val dependencyResult = DependencyResult()
        dependencyResult.partitionTs = batchTs
        dependencyResult.partitions = parentParts.toTypedArray()

        service.saveAll(childPart.id, arrayOf(dependencyResult), true)

        val result = service.saveAll(childPart.id, arrayOf(dependencyResult), true)

        Truth.assertThat(result.isSuccess).isTrue()
        Truth.assertThat(result.isError).isFalse()
    }

    @Test
    @Order(3)
    fun saveFailNotOverwrite() {
        val batchTs = timestamps!!.last()
        val childPart = partitions!!.first()
        val parentParts = partitions!!.slice(0 until partitions!!.size - 1)

        val dependencyResult = DependencyResult()
        dependencyResult.partitionTs = batchTs
        dependencyResult.partitions = parentParts.toTypedArray()

        service.saveAll(childPart.id, arrayOf(dependencyResult), true)

        val result = service.saveAll(childPart.id, arrayOf(dependencyResult), false)

        Truth.assertThat(result.isSuccess).isFalse()
        Truth.assertThat(result.isError).isTrue()
    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(PartitionDependencyDAOTest::class.java)

        @Container
        var MY_SQL_DB: MySQLContainer<*> = TestHelper.mysqlContainer()

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