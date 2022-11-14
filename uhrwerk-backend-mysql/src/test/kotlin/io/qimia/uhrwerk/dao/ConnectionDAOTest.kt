package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.TestHelper
import io.qimia.uhrwerk.common.model.ConnectionModel
import io.qimia.uhrwerk.config.ConnectionBuilder
import io.qimia.uhrwerk.repo.HikariCPDataSource
import io.qimia.uhrwerk.TestData
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class ConnectionDAOTest {

    private val service = ConnectionDAO()

    @AfterEach
    fun cleanUp() {
        TestHelper.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
    }

    @Test
    fun save() {
        val result = service.save(TestData.connection("Connection-Test"), true)
        Assertions.assertTrue(result.isSuccess)
        Assertions.assertNotNull(result.newConnection)
        Assertions.assertNotNull(result.newConnection.id)
        LOGGER.info(result.newConnection!!.toString())
    }


    @Test
    fun insertFullConnection() {
        val conns = mutableListOf<ConnectionModel>()

        val connection1 = ConnectionBuilder()
            .name(CONN_NAMES[0])
            .s3()
            .path("S3Path")
            .secretId("ID")
            .secretKey("key")
            .done()
            .build()
        conns.add(connection1)

        val connection2 = ConnectionBuilder()
            .name(CONN_NAMES[1])
            .jdbc()
            .jdbcUrl("url")
            .jdbcDriver("driver")
            .user("user")
            .pass("pass2")
            .done()
            .build()

        conns.add(connection2)

        val connection3 =
            ConnectionBuilder().name(CONN_NAMES[2]).file().path("filePath").done().build()
        conns.add(connection3)

        conns.forEach {
            LOGGER.info(it.toString())
            val result = service.save(it, true)
            Assertions.assertTrue(result.isSuccess)
            Assertions.assertNotNull(result.newConnection)
            Assertions.assertNotNull(result.newConnection.id)
            LOGGER.info(result.newConnection.toString())
        }

    }

    companion object {
        private val CONN_NAMES = arrayOf("S3", "JDBC", "file")

        private val LOGGER = LoggerFactory.getLogger(ConnectionDAOTest::class.java)

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