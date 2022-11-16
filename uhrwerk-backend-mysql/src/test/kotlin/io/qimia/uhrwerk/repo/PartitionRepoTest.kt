package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.LocalDateTime

@Testcontainers
internal class PartitionRepoTest {

    private val repo = PartitionRepo()

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var target: TargetModel? = null

    private var partition: Partition? = null
    private var partitionTs: LocalDateTime? = null


    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("PARTITION_", LOGGER)
        TestUtils.cleanData("TARGET", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-PartitionTest"))
        table = TableRepo().save(TestData.table("Table-PartitionTest"))
        target = TargetRepo().save(TestData.target(connection!!.id!!, table!!.id!!))

        partitionTs = LocalDateTime.of(2022, 7, 20, 12, 0)

        partition = repo.save(
            TestData.partition(
                targetId = target!!.id!!,
                partitionTs = partitionTs!!
            )
        )
    }


    @Test
    fun save() {
        Truth.assertThat(partition).isNotNull()
        Truth.assertThat(partition!!.partitionTs).isNotNull()

    }

    @Test
    fun getById() {
        val partition1 = repo.getById(partition!!.id!!)
        Truth.assertThat(partition1).isNotNull()
        Truth.assertThat(partition1).isEqualTo(partition)
    }

    @Test
    fun getUniqueColumns() {
        val partition1 = repo.getUniqueColumns(target!!.id!!, partitionTs!!)
        Truth.assertThat(partition1).isNotNull()
        Truth.assertThat(partition1).isEqualTo(partition)
    }

    @Test
    fun getLatestByTargetId() {
        val timestamps = TestData.timestamps(15)
        val partitions = timestamps.map {
            TestData.partition(
                targetId = target!!.id!!,
                partitionTs = it
            )
        }

        val partitionIds = partitions.map { repo.save(it) }
        Truth.assertThat(partitionIds).hasSize(5)

        val latestPartition = repo.getLatestByTargetId(target!!.id!!)
        Truth.assertThat(latestPartition).isNotNull()
        Truth.assertThat(latestPartition!!.targetId).isEqualTo(target!!.id)

        Truth.assertThat(latestPartition!!.partitionTs).isEqualTo(timestamps[4])

    }

    @Test
    fun getAllByTargetTs() {
        val timestamps = TestData.timestamps(15)
        val partitions = timestamps.map {
            TestData.partition(
                targetId = target!!.id!!,
                partitionTs = it
            )
        }
        val partitionIds = partitions.map { repo.save(it) }


        Truth.assertThat(partitionIds).hasSize(5)
        val partitions1 = repo.getAllByTargetTs(target!!.id!!, timestamps)
        Truth.assertThat(partitions1).isNotEmpty()
        Truth.assertThat(partitions1).hasSize(5)

        partitions1.zip(timestamps).forEach { (part, ts) ->
            Truth.assertThat(part.targetId).isEqualTo(target!!.id)
            Truth.assertThat(part.partitionTs).isEqualTo(ts)
        }

    }

    @Test
    fun deleteById() {
        val effect = PartitionRepo().deleteById(partition!!.id!!)
        Truth.assertThat(effect).isNotNull()
        Truth.assertThat(effect!!).isEqualTo(1)
    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(PartitionRepoTest::class.java)

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