package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class DependencyRepoTest {

    private var table: TableModel? = null

    private var connection: ConnectionModel? = null

    private var dependencyTarget: TargetModel? = null
    private var dependencyTable: TableModel? = null

    private var dependency: DependencyModel? = null

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("DEPENDENCY", LOGGER)
        TestUtils.cleanData("TARGET", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun addDeps() {
        table = TableRepo().save(TestData.table("Table-DependencyRepoTest"))

        connection =
            ConnectionRepo().save(TestData.connection("Dependency-Connection-DependencyRepoTest"))

        dependencyTable = TableRepo().save(TestData.table("Dependency-Table-DependencyRepoTest"))

        dependencyTarget =
            TargetRepo().save(TestData.target(dependencyTable!!.id!!, connection!!.id!!))

        dependency = DependencyRepo().save(
            TestData.dependency(
                table!!.id!!,
                dependencyTarget!!.id!!,
                dependencyTable!!.id!!
            )
        )


    }

    @Test
    fun save() {
        assertThat(dependency).isNotNull()
    }

    @Test
    fun getById() {
        val dependency = DependencyRepo().getById(dependency!!.id!!)
        assertThat(dependency).isNotNull()
        assertThat(dependency!!.tableId).isEqualTo(table!!.id)
        assertThat(dependency!!.dependencyTargetId).isEqualTo(dependencyTarget!!.id)
    }

    @Test
    fun getByTableId() {
        val dependency = DependencyRepo().getByTableId(table!!.id!!)
        assertThat(dependency).isNotNull()
        assertThat(dependency!!).isNotEmpty()
        assertThat(dependency!![0].tableId).isEqualTo(table!!.id)
        assertThat(dependency!![0].dependencyTargetId).isEqualTo(dependencyTarget!!.id)
    }

    @Test
    fun deleteById() {
        val effect = DependencyRepo().deleteById(dependency!!.id!!)
        assertThat(effect).isNotNull()
        assertThat(effect!!).isEqualTo(1)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(DependencyRepoTest::class.java)

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