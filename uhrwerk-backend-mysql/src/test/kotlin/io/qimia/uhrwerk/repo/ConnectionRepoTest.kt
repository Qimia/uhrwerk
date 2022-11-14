package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import io.qimia.uhrwerk.TestHelper
import io.qimia.uhrwerk.common.model.ConnectionModel
import io.qimia.uhrwerk.common.model.HashKeyUtils
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
        TestHelper.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
        connection = repo.save(TestData.connection("Connection-Test"))
    }

    @Test
    fun save() {
        Truth.assertThat(connection).isNotNull()
    }

    @Test
    fun getById() {
        val conn = repo.getById(connection!!.id)
        Truth.assertThat(conn).isNotNull()
        Truth.assertThat(conn!!.name).isEqualTo("Connection-Test")
    }

    @Test
    fun getByHash() {
        val hashKey = HashKeyUtils.connectionKey(TestData.connection("Connection-Test"))
        val connections = repo.getByHashKey(hashKey)
        Truth.assertThat(connections).isNotNull()
        Truth.assertThat(connections).hasSize(1)
        Truth.assertThat(connections).hasSize(1)
        Truth.assertThat(connections!!.first().name).isEqualTo("Connection-Test")
        Truth.assertThat(connections!!.first()).isEqualTo(connection)
    }

    @Test
    fun getByHashKey() {
        val hashKey = HashKeyUtils.connectionKey(ConnectionModel.builder().name("Connection-Test").build())
        val conns = repo.getByHashKey(hashKey)
        Truth.assertThat(conns).isNotEmpty()
        Truth.assertThat(conns).hasSize(1)
        Truth.assertThat(conns[0]).isEqualTo(connection)
    }

    @Test
    fun deactivateById() {
        val effect = repo.deactivateById(connection!!.id)
        Truth.assertThat(effect).isNotNull()
        Truth.assertThat(effect!!).isEqualTo(1)

        val hashKey = HashKeyUtils.connectionKey(ConnectionModel.builder().name("Connection-Test").build())
        val conns = repo.getByHashKey(hashKey)
        Truth.assertThat(conns).isEmpty()

        val conn = repo.getById(connection!!.id)
        Truth.assertThat(conn).isNotNull()
        Truth.assertThat(conn!!.deactivatedTs).isNotNull()

        val conn1 = repo.save(TestData.connection("Connection-Test"))
        Truth.assertThat(conn).isNotNull()
        Truth.assertThat(conn).isNotEqualTo(connection)

        val conns1 = repo.getByHashKey(hashKey)
        Truth.assertThat(conns1).isNotEmpty()
        Truth.assertThat(conns1).hasSize(1)
        Truth.assertThat(conns1[0].name).isEqualTo("Connection-Test")
        Truth.assertThat(conns1[0].deactivatedTs).isNull()

    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(ConnectionRepoTest::class.java)

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