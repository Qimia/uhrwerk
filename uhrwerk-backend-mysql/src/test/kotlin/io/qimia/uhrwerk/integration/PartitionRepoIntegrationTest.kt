package io.qimia.uhrwerk.integration

import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.TestData
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import io.qimia.uhrwerk.repo.*
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import java.time.LocalDateTime

internal class PartitionRepoIntegrationTest {

    private val repo = PartitionRepo()

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var target: TargetModel? = null

    private var partition: Partition? = null
    private var partitionTs: LocalDateTime? = null


    @AfterEach
    fun cleanUp() {
    }

    @BeforeEach
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-PartitionIntTest"))
        table = TableRepo().save(TestData.table("Table-PartitionIntTest"))

        println("saved table...")
        println(table)
        TestData.target(table!!.id!!, connection!!.id!!).let {
            println("saving target: $it")
            target = TargetRepo().save(it)
        }

        partitionTs = LocalDateTime.of(2022, 7, 20, 12, 0)

        partition = repo.save(
            TestData.partition(
                targetId = target!!.id!!,
                partitionTs = partitionTs!!
            )
        )
    }

    @Test
    fun partitioned() {

        val times = listOf(
            LocalDateTime.of(2023, 1, 23, 12, 0),
            LocalDateTime.of(2023, 1, 24, 12, 0),
            LocalDateTime.of(2023, 1, 25, 12, 0),
            LocalDateTime.of(2023, 1, 26, 12, 0)
        )

        println("Writing partitions... for target ${target!!.id}")

        for (i in 1..4) {
            val partValues = mapOf("part_col1" to "part_val1", "part_col2" to "part_val$i")

            val partitions = times.map {
                Partition(
                    targetId = target!!.id,
                    partitionTs = it,
                    partitionValues = partValues
                )
            }
            repo.save(partitions).let { part ->
                assertThat(part).hasSize(4)
                assertThat(part?.all { it.id != null }).isTrue()
            }
        }

    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PartitionRepoIntegrationTest::class.java)

        @BeforeAll
        @JvmStatic
        fun setUp() {
            Slf4jLogConsumer(LOGGER)
            HikariCPDataSource.initConfig(
                "jdbc:mysql://localhost:53306/UHRWERK_METASTORE",
                "UHRWERK_USER",
                "Xq92vFqEKF7TB8H9"
            )
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            HikariCPDataSource.close()
        }
    }
}