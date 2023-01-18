package io.qimia.uhrwerk.integration

import TestUtils
import TestUtils.filePath
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.dao.ConnectionDAO
import io.qimia.uhrwerk.repo.HikariCPDataSource
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class ConnectionIntTest {

    private val service = ConnectionDAO()

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
    }

    @Test
    fun read() {
        val connections = YamlConfigReader().readConnections(YAML_FILE)
        assertThat(connections).isNotNull()
        assertThat(connections!!.toList()).hasSize(4)
    }


    @Test
    fun insertFullConnection() {
        val connections = YamlConfigReader().readConnections(YAML_FILE)
        val results = connections.map { service.save(it,true) }
        connections.forEach { assertThat(it!!.id).isNotNull() }
        results.forEach {
            assertThat(it).isNotNull()
            assertThat(it.isSuccess).isTrue()
            assertThat(it.newConnection).isNotNull()
        }
    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(ConnectionIntTest::class.java)

        @Container
        var MY_SQL_DB: MySQLContainer<*> = TestUtils.mysqlContainer()

        private const val CONNECTION_YAML = "config/connection-config-new.yml"
        private val YAML_FILE = filePath(CONNECTION_YAML)!!

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