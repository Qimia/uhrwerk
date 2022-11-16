package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.TestData
import TestUtils
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.common.metastore.builders.ConnectionModelBuilder
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class ConnectionRepoTest {

    private val repo = ConnectionRepo()

    private var connection: ConnectionModel? = null

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
        connection = repo.save(TestData.connection("Connection-Test"))
    }

    @Test
    fun save() {
        assertThat(connection).isNotNull()
        assertThat(connection!!.id).isNotNull()
    }

    @Test
    fun getById() {
        val conn = repo.getById(connection!!.id!!)
        assertThat(conn).isNotNull()
        assertThat(conn!!.name).isEqualTo("Connection-Test")
    }

    @Test
    fun getByHash() {
        val hashKey = HashKeyUtils.connectionKey(TestData.connection("Connection-Test"))
        val connections = repo.getByHashKey(hashKey)
        assertThat(connections).isNotNull()
        assertThat(connections!!.name).isEqualTo("Connection-Test")
        assertThat(connections!!).isEqualTo(connection)
    }

    @Test
    fun getByHashKey() {
        val hashKey = HashKeyUtils.connectionKey(ConnectionModelBuilder().name("Connection-Test").build())
        val conns = repo.getByHashKey(hashKey)
        assertThat(conns).isEqualTo(connection)
    }

    @Test
    fun deactivateById() {
        val effect = repo.deactivateById(connection!!.id!!)
        assertThat(effect).isNotNull()
        assertThat(effect!!).isEqualTo(1)

        val hashKey = HashKeyUtils.connectionKey(ConnectionModelBuilder().name("Connection-Test").build())
        val conns = repo.getByHashKey(hashKey)

        val conn = repo.getById(connection!!.id!!)
        assertThat(conn).isNotNull()
        assertThat(conn!!.deactivatedTs).isNotNull()

        val conn1 = repo.save(TestData.connection("Connection-Test"))
        assertThat(conn).isNotNull()
        assertThat(conn).isNotEqualTo(connection)

        val conn2 = repo.getByHashKey(hashKey)
        assertThat(conn2?.name).isEqualTo("Connection-Test")
        assertThat(conn2?.deactivatedTs).isNull()

    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(ConnectionRepoTest::class.java)

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