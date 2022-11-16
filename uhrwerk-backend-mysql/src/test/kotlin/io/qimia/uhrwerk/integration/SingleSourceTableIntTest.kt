package io.qimia.uhrwerk.integration

import TestUtils
import TestUtils.filePath
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.dao.ConnectionDAO
import io.qimia.uhrwerk.dao.SourceDAO
import io.qimia.uhrwerk.dao.TableDAO
import io.qimia.uhrwerk.dao.TargetDAO
import io.qimia.uhrwerk.repo.DependencyRepoTest
import io.qimia.uhrwerk.repo.HikariCPDataSource
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class SingleSourceTableIntTest {

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
    fun saveTableOnly() {
        val connsFile = filePath("config/single_source_table/connections-config.yml")
        val connections = YamlConfigReader().readConnections(connsFile)

        assertThat(connections).isNotNull()
        assertThat(connections.toList()).hasSize(2)

        val tableFile = filePath("config/single_source_table/single-source-table-config.yml")
        val table = YamlConfigReader().readTable(tableFile)

        assertThat(table).isNotNull()
        assertThat(table.sources).isNotNull()
        assertThat(table.sources!!.toList()).hasSize(1)
        assertThat(table.targets).isNotNull()
        assertThat(table.targets!!.toList()).hasSize(1)

        connections.forEach { connService.save(it, true) }
        val result = tableService.save(table, true)

        println(result)

        assertThat(result).isNotNull()
        assertThat(result.isSuccess).isTrue()
        assertThat(table.id).isNotNull()
        assertThat(table).isEqualTo(result.newResult)

    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(SingleSourceTableIntTest::class.java)

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