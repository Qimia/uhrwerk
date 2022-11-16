package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import TestUtils
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class HikariCPDataSourceTest {

    @Test
    fun getConnection() {
        Truth.assertThat(MY_SQL_DB.isRunning).isTrue()

        val connection = HikariCPDataSource.connection
        connection.use {
            val stmt = connection.createStatement()
            stmt.use {
                val rs = stmt.executeQuery("Show tables")
                while (rs.next()) {
                    LOGGER.info(rs.getString(1))
                }
            }
        }

    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(HikariCPDataSourceTest::class.java)

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