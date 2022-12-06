package io.qimia.uhrwerk.integration

import TestUtils
import TestUtils.filePath
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.dao.ConnectionDAO
import io.qimia.uhrwerk.dao.SourceDAO
import io.qimia.uhrwerk.dao.TableDAO
import io.qimia.uhrwerk.dao.TargetDAO
import io.qimia.uhrwerk.repo.HikariCPDataSource
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class DAGIntegrationTest {

    private val connService = ConnectionDAO()
    private val tableService = TableDAO()

    private val targetService = TargetDAO()
    private val sourceService = SourceDAO()


    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("DEPENDENCY", LOGGER)
        TestUtils.cleanData("TARGET", LOGGER)
        TestUtils.cleanData("SOURCE", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
    }

    @Test
    fun test() {
        val dagFile = filePath("config/dag-config-new.yml")
        val dag = YamlConfigReader().readDag(dagFile)


        assertThat(dag).isNotNull()
        assertThat(dag.connections).isNotNull()
        assertThat(dag.connections?.toList()).hasSize(2)

        val connResults = dag.connections?.map { connService.save(it, true) }
        connResults?.forEach { assertThat(it.isSuccess).isTrue() }

        assertThat(dag.tables).isNotNull()
        assertThat(dag.tables?.toList()).hasSize(2)
        assertThat(dag.tables!![1].dependencies).isNotNull()
        assertThat(dag.tables!![1].dependencies?.toList()).hasSize(1)

        val tableResults = dag.tables?.map { tableService.save(it, true) }
        tableResults?.forEach { LOGGER.info(it.toString()) }
        tableResults?.forEach { assertThat(it.isSuccess).isTrue() }
    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(DAGIntegrationTest::class.java)

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