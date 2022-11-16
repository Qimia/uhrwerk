package io.qimia.uhrwerk.dao

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import io.qimia.uhrwerk.common.metastore.config.PartitionService
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import io.qimia.uhrwerk.repo.*
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
internal class PartitionDAOTest {

    private val service: PartitionService = PartitionDAO()

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var target: TargetModel? = null


    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("PARTITION_", LOGGER)
        TestUtils.cleanData("TARGET", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
        connection = ConnectionRepo().save(TestData.connection("Connection-PartitionDAOTest"))
        table = TableRepo().save(TestData.table("Table-PartitionTest"))
        target = TargetRepo().save(TestData.target(connection!!.id!!, table!!.id!!))
    }

    @Test
    @Order(1)
    fun save() {
        val partition = TestData.partition(target!!.id!!)
        val result = service.save(partition, false)

        Truth.assertThat(result.isSuccess).isTrue()
        Truth.assertThat(result.isError).isFalse()
        Truth.assertThat(result.newResult).isNotNull()
        Truth.assertThat(result.newResult).isEqualTo(partition)
        Truth.assertThat(result.oldResult).isNull()
    }

    @Test
    @Order(2)
    fun saveOverwrite() {
        val partitionTs = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)

        val partition = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result = service.save(partition, false)

        val partition1 = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )
        partition1.partitionUnit = PartitionUnit.MINUTES
        partition1.partitionSize = 15

        val result1 = service.save(partition1, true)

        Truth.assertThat(result1.isSuccess).isTrue()
        Truth.assertThat(result1.isError).isFalse()
        Truth.assertThat(result1.newResult).isNotNull()
        Truth.assertThat(result1.oldResult).isNotNull()
        Truth.assertThat(result1.oldResult).isEqualTo(result.newResult)
        Truth.assertThat(result1.oldResult.id).isEqualTo(result.newResult.id)

        val partition2 = service.getById(result1.oldResult.id)
        Truth.assertThat(partition2).isNull()
    }

    @Test
    @Order(3)
    fun saveFailNotOverwrite() {
        val partitionTs = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)

        val partition = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result = service.save(partition, false)

        val partition1 = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )
        partition1.partitionUnit = PartitionUnit.MINUTES
        partition1.partitionSize = 15

        val result1 = service.save(partition1, false)

        Truth.assertThat(result1.isSuccess).isFalse()
        Truth.assertThat(result1.isError).isFalse()
        Truth.assertThat(result1.newResult).isNull()
        Truth.assertThat(result1.oldResult).isNotNull()
        Truth.assertThat(result1.oldResult).isEqualTo(result.newResult)
        Truth.assertThat(result1.oldResult.id).isEqualTo(result.newResult.id)

        val partition2 = service.getById(result1.oldResult.id)
        Truth.assertThat(partition2).isNotNull()
    }

    @Test
    @Order(4)
    fun saveSameNotOverwrite() {
        val partitionTs = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)

        val partition = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result = service.save(partition, false)

        val partition1 = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result1 = service.save(partition1, false)

        Truth.assertThat(result1.isSuccess).isTrue()
        Truth.assertThat(result1.isError).isFalse()
        Truth.assertThat(result1.newResult).isNotNull()
        Truth.assertThat(result1.oldResult).isNotNull()
        Truth.assertThat(result1.oldResult).isEqualTo(result.newResult)
        Truth.assertThat(result1.oldResult).isEqualTo(result1.newResult)

        val partition2 = service.getById(result1.oldResult.id)
        Truth.assertThat(partition2).isNotNull()
    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(PartitionDAOTest::class.java)

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