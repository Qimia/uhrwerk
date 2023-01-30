package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.TestData
import TestUtils
import com.google.common.truth.Truth.assertThat
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
        assertThat(result).isNotNull()
        if (result != null) {
            assertThat(result.isSuccess).isTrue()
            assertThat(result.isError).isFalse()
            assertThat(result.newResult).isNotNull()
            assertThat(result.newResult).isEqualTo(partition)
            assertThat(result.oldResult).isNull()
        }
    }

    @Test
    @Order(2)
    fun saveOverwrite() {
        val partitionTs = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)

        val partition = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result = service.save(partition, false)!!

        val partition1 = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )
        partition1.partitionUnit = PartitionUnit.MINUTES
        partition1.partitionSize = 15

        val result1 = service.save(partition1, true)

        assertThat(result1).isNotNull()

        if (result1 != null) {
            assertThat(result1.isSuccess).isTrue()
            assertThat(result1.isError).isFalse()
            assertThat(result1.newResult).isNotNull()
            assertThat(result1.oldResult).isNotNull()
            assertThat(result1.oldResult).isEqualTo(result.newResult!!)
            assertThat(result1.oldResult!!.id).isEqualTo(result.newResult!!.id)
            val partition2 = service.getById(result1.oldResult!!.id!!)
            assertThat(partition2).isNull()
        }

    }

    @Test
    @Order(3)
    fun saveFailNotOverwrite() {
        val partitionTs = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)

        val partition = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result = service.save(partition, false)!!

        val partition1 = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )
        partition1.partitionUnit = PartitionUnit.MINUTES
        partition1.partitionSize = 15

        val result1 = service.save(partition1, false)

        assertThat(result1).isNotNull()

        if (result1 != null) {
            assertThat(result1.isSuccess).isFalse()
            assertThat(result1.isSuccess).isFalse()
            assertThat(result1.isError).isFalse()
            assertThat(result1.newResult).isNull()
            assertThat(result1.oldResult).isNotNull()
            assertThat(result1.oldResult).isEqualTo(result.newResult)
            assertThat(result1.oldResult).isEqualTo(result.newResult)
            assertThat(result1.oldResult!!.id).isEqualTo(result.newResult!!.id)
            val partition2 = service.getById(result1.oldResult!!.id!!)
            assertThat(partition2).isNotNull()
        }

    }

    @Test
    @Order(4)
    fun saveSameNotOverwrite() {
        val partitionTs = LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)

        val partition = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result = service.save(partition, false)!!

        val partition1 = TestData.partition(
            targetId = target!!.id!!,
            partitionTs = partitionTs
        )

        val result1 = service.save(partition1, false)

        assertThat(result1).isNotNull()

        if (result1 != null) {
            assertThat(result1.isSuccess).isTrue()
            assertThat(result1.isError).isFalse()
            assertThat(result1.newResult).isNotNull()
            assertThat(result1.oldResult).isNotNull()
            assertThat(result1.oldResult).isEqualTo(result.newResult)
            assertThat(result1.oldResult).isEqualTo(result1.newResult)

            val partition2 = service.getById(result1.oldResult!!.id!!)
            assertThat(partition2).isNotNull()
        }


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