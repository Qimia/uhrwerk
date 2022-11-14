package io.qimia.uhrwerk.repo


import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import io.qimia.uhrwerk.TestHelper
import io.qimia.uhrwerk.common.model.ConnectionModel
import io.qimia.uhrwerk.common.model.Partition
import io.qimia.uhrwerk.common.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class PartitionDependencyRepoTest {

    private val repo = PartitionDependencyRepo()

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var target: TargetModel? = null

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
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-PartitionTest"))
        table = TableRepo().save(TestData.table("Table-PartitionTest"))
        target = TargetRepo().save(TestData.target(connection!!.id, table!!.id))

        val parts = TestData.timestamps(15).map {
            TestData.partition(
                targetId = target!!.id,
                partitionTs = it
            )
        }

        partitions = PartitionRepo().save(parts)
    }

    @Test
    fun save() {
        // childPartition the last one in the list
        val childPartId = partitions!!.last().id
        val parentPartIds = partitions!!.slice(0 until partitions!!.size - 1).map { it.id }

        val partDeps = TestData.partitionDependencies(childPartId, parentPartIds)
        val partDepIds = repo.save(partDeps)
        Truth.assertThat(partDepIds).isNotNull()
        Truth.assertThat(partDepIds).hasSize(partitions!!.size - 1)

        val parentParts = PartitionRepo().getAllParentPartitions(childPartId)
        Truth.assertThat(parentParts).isNotNull()
        Truth.assertThat(parentParts).hasSize(partitions!!.size - 1)

        val parentPartIds1 = parentParts.map { it.id }
        Truth.assertThat(parentPartIds1).containsExactlyElementsIn(parentPartIds)
    }

    @Test
    fun testSave() {
    }

    @Test
    fun getById() {
    }

    @Test
    fun deleteByPartID() {
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PartitionDependencyRepoTest::class.java)

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