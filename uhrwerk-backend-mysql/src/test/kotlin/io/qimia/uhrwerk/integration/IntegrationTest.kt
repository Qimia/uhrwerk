package io.qimia.uhrwerk.integration

import TestUtils
import TestUtils.filePath
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.dao.ConnectionDAO
import io.qimia.uhrwerk.dao.TableDAO
import io.qimia.uhrwerk.repo.HikariCPDataSource
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class IntegrationTest {

    private val connService = ConnectionDAO()
    private val tableService = TableDAO()

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
    fun connectionsOverwrite() {
        val connsFile = filePath("config/uhrwerk_examples/connection-config-new.yml")
        val connections = YamlConfigReader().readConnections(connsFile)

        assertThat(connections).isNotNull()
        assertThat(connections.toList()).hasSize(5)

        val connResults = connections?.map { connService.save(it, false) }
        connResults?.forEach {
            assertThat(it.isSuccess).isTrue()
            assertThat(it.newConnection.id).isNotNull()
        }

        val connections2 = YamlConfigReader().readConnections(connsFile)
        val connResults2 = connections2?.map { connService.save(it, false) }
        connResults2?.forEach {
            assertThat(it.isSuccess).isFalse()
            assertThat(it.oldConnection).isNotNull()
        }
    }

    @Test
    fun tableSave() {

        val connsFile = filePath("config/uhrwerk_examples/connection-config-new.yml")
        val connections = YamlConfigReader().readConnections(connsFile)
        connections?.forEach { connService.save(it, false) }

        val tableFile = filePath("config/uhrwerk_examples/table_category_1.0.yml")
        val table = YamlConfigReader().readTable(tableFile)

        assertThat(table.partitionColumns).isNotNull()
        assertThat(table.partitionColumns).hasLength(2)
        assertThat(table.partitionColumns).isEqualTo(arrayOf("column1", "column2"))

        assertThat(table.sources).isNotNull()
        assertThat(table.sources!!.toList()).isNotEmpty()
        table.sources!!.forEach {
            assertThat(it.connection).isNotNull()
            assertThat(it.connection!!.name).isNotNull()
        }


        assertThat(table.targets).isNotNull()
        assertThat(table.targets!!.toList()).hasSize(2)

        table.targets!!.forEach {
            assertThat(it.connection).isNotNull()
            assertThat(it.connection!!.name).isNotNull()
        }

        assertThat(table).isNotNull()
        assertThat(table.sources).isNotNull()
        assertThat(table.targets).isNotNull()

        //Save Table
        val tableResult = tableService.save(table, false)

        assertThat(tableResult.isSuccess).isTrue()
        assertThat(table.id).isNotNull()

        assertThat(tableResult.sourceResults).isNotNull()
        assertThat(tableResult.sourceResults.toList()).hasSize(table.sources!!.size)
        tableResult.sourceResults.forEach {
            assertThat(it.isSuccess).isTrue()
            assertThat(it.newResult.id).isNotNull()
            assertThat(it.newResult.tableId).isEqualTo(table.id)
            assertThat(it.newResult.connectionId).isNotNull()
        }

        assertThat(tableResult.targetResult).isNotNull()
        assertThat(tableResult.targetResult.isSuccess).isTrue()
        assertThat(tableResult.targetResult.storedTargets.toList()).hasSize(table.targets!!.size)
        tableResult.targetResult.storedTargets.forEach {
            assertThat(it.id).isNotNull()
            assertThat(it.tableId).isEqualTo(table.id)
            assertThat(it.connectionId).isNotNull()
        }
    }

    @Test
    fun tableOverwrite() {

        val connsFile = filePath("config/uhrwerk_examples/connection-config-new.yml")
        val connections = YamlConfigReader().readConnections(connsFile)
        connections?.forEach { connService.save(it, false) }

        val tableFile = filePath("config/uhrwerk_examples/table_category_1.0.yml")
        val table = YamlConfigReader().readTable(tableFile)
        println(table)

        //Save table first time
        val tableResult = tableService.save(table, false)
        println(tableResult.newResult)

        //Read and save table second time
        val table2 = YamlConfigReader().readTable(tableFile)
        val tableResult2 = tableService.save(table2, false)

        assertThat(tableResult2.isSuccess).isTrue()
        println(tableResult2)

    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(IntegrationTest::class.java)

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