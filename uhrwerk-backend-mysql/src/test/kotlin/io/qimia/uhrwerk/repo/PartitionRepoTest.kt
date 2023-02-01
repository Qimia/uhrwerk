package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.TestData
import TestUtils
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import io.qimia.uhrwerk.repo.RepoUtils.toJson
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
        assertThat(partition).isNotNull()
        assertThat(partition!!.partitionTs).isNotNull()

    }

    @Test
    fun partitioned() {
        partitionTs = LocalDateTime.now()
        val partValues = mapOf("part_col1" to "part_val1", "part_col2" to "part_val2")

        val partition = Partition(
            targetId = target!!.id,
            partitionTs = partitionTs,
            partitionValues = partValues
        )
        repo.save(partition)
        assertThat(partition.id).isNotNull()

        repo.getById(partition.id!!)?.let {
            assertThat(it.partitionValues).isEqualTo(partValues)
        }

        val partsJson = toJson(partValues)

        repo.getLatestByTargetIdPartitionValues(target!!.id!!, partsJson)?.let {
            assertThat(it[0].partitionValues).isEqualTo(partValues)
        }

        val partJson2 = toJson(mapOf("part_col1" to "part_val1"))

        repo.getLatestByTargetIdPartitionValues(target!!.id!!, partJson2)?.let {
            assertThat(it[0].partitionValues).isEqualTo(partValues)
            println(it)
        }

        val partJson3 = toJson(mapOf("part_col3" to "part_val1"))
        val noPartition =
            repo.getLatestByTargetIdPartitionValues(target!!.id!!, partJson3)
        assertThat(noPartition).isNotNull()
        assertThat(noPartition).isEmpty()

    }

    @Test
    fun getById() {
        val partition1 = repo.getById(partition!!.id!!)
        assertThat(partition1).isNotNull()
        assertThat(partition1).isEqualTo(partition)
        assertThat(partition1?.partitionValues).isNotNull()
        assertThat(partition1?.partitionValues).isEqualTo(mapOf("col1" to "val1", "col2" to 20))

    }

    @Test
    fun getUniqueColumns() {
        val partition1 = repo.getUniqueColumns(target!!.id!!, partitionTs!!)
        assertThat(partition1).isNotNull()
        assertThat(partition1).isEqualTo(partition)
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
        assertThat(partitionIds).hasSize(5)

        val latestPartition = repo.getLatestByTargetId(target!!.id!!)
        assertThat(latestPartition).isNotNull()
        assertThat(latestPartition!!.targetId).isEqualTo(target!!.id)

        assertThat(latestPartition.partitionTs).isEqualTo(timestamps[4])

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


        assertThat(partitionIds).hasSize(5)
        val partitions1 = repo.getAllByTargetTs(target!!.id!!, timestamps)
        assertThat(partitions1).isNotEmpty()
        assertThat(partitions1).hasSize(5)

        partitions1.zip(timestamps).forEach { (part, ts) ->
            assertThat(part.targetId).isEqualTo(target!!.id)
            assertThat(part.partitionTs).isEqualTo(ts)
        }

    }

    @Test
    fun deleteById() {
        val effect = PartitionRepo().deleteById(partition!!.id!!)
        assertThat(effect).isNotNull()
        assertThat(effect!!).isEqualTo(1)
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