package io.qimia.uhrwerk

import TestUtils
import com.google.common.truth.Truth
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.IOException
import java.sql.DriverManager
import java.sql.SQLException

@Testcontainers
class TestContainersMySQL {
    @Test
    fun dbRunningTest() {
        Truth.assertThat(MY_SQL_DB.isRunning).isTrue()
        try {
            val connection = DriverManager.getConnection(
                MY_SQL_DB.jdbcUrl,
                "UHRWERK_USER",
                "Xq92vFqEKF7TB8H9"
            )
            val stmt = connection.createStatement()
            //Retrieving the data
            val rs = stmt.executeQuery("Show tables")
            while (rs.next()) {
                logger.info(rs.getString(1))
            }
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
        try {
            val execResult = MY_SQL_DB.execInContainer(
                "mysql",
                "-uUHRWERK_USER",
                "-pXq92vFqEKF7TB8H9",
                "UHRWERK_METASTORE",
                "-e show tables"
            )
            logger.info(execResult.stderr)
            logger.info(execResult.stdout)
        } catch (e: IOException) {
            throw RuntimeException(e)
        } catch (e: InterruptedException) {
            throw RuntimeException(e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TestContainersMySQL::class.java)

        @Container
        var MY_SQL_DB: MySQLContainer<*> = TestUtils.mysqlContainer()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            val logConsumer = Slf4jLogConsumer(logger)
            MY_SQL_DB.followOutput(logConsumer)
        }
    }
}