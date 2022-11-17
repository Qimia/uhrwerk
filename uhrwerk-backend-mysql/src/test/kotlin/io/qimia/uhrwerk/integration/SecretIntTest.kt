package io.qimia.uhrwerk.integration

import TestUtils
import TestUtils.filePath
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.dao.SecretDAO
import io.qimia.uhrwerk.repo.HikariCPDataSource
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class SecretIntTest {

    private val service = SecretDAO()

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("SECRET_", LOGGER)
    }

    @BeforeEach
    fun saveData() {
    }

    @Test
    fun read() {
        val secrets = YamlConfigReader().readSecrets(YAML_FILE)
        assertThat(secrets).isNotNull()
        assertThat(secrets).isNotNull()
        assertThat(secrets?.toList()).hasSize(2)
    }


    @Test
    fun insertFullConnection() {
        val secrets = YamlConfigReader().readSecrets(YAML_FILE)
        val results = secrets?.map { service.save(it, true)!! }
        secrets?.forEach { assertThat(it!!.id).isNotNull() }
        results?.forEach {
            assertThat(it).isNotNull()
            assertThat(it.isSuccess).isTrue()
            assertThat(it.isError).isFalse()
            assertThat(it.newSecret).isNotNull()
            assertThat(it.oldSecret).isNull()
        }
    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(SecretIntTest::class.java)

        @Container
        var MY_SQL_DB: MySQLContainer<*> = TestUtils.mysqlContainer()

        private const val CONNECTION_YAML = "config/secrets-config.yml"
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