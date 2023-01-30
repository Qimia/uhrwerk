package io.qimia.uhrwerk.dao

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.common.metastore.builders.ConnectionModelBuilder
import io.qimia.uhrwerk.common.metastore.builders.TargetModelBuilder
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.dao.TargetDAO.Companion.compareTargets
import io.qimia.uhrwerk.repo.ConnectionRepo
import io.qimia.uhrwerk.repo.HikariCPDataSource
import io.qimia.uhrwerk.repo.TableRepo
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.SQLException

@Testcontainers
class TargetDAOTest {

    private val service = TargetDAO()

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null

    @BeforeEach
    @Throws(SQLException::class)
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-TargetTest"))
        table = TableRepo().save(TestData.table("Table-TargetTest"))
    }

    @AfterEach
    @Throws(SQLException::class)
    fun cleanUp() {
        TestUtils.cleanData("TARGET", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @Test
    fun compareTest() {
        val connA = ConnectionModel()
        connA.name = "connA"

        val tarA = TargetModelBuilder()
            .format("jdbc")
            .tableId(123L)
            .connection(connA)
            .build()


        val connAFull = ConnectionModelBuilder()
            .name("connA")
            .jdbcDriver("somedriver")
            .jdbcUrl("someurl")
            .jdbcUser("root")
            .jdbcPass("somePass")
            .build()

        val tarB = TargetModelBuilder()
            .format("jdbc")
            .tableId(123L)
            .connection(connAFull)
            .build()

        Assertions.assertTrue(compareTargets(tarB, tarA))
        val tarA1 = TargetModelBuilder()
            .format("parquet")
            .tableId(123L)
            .connection(connA)
            .build()

        Assertions.assertFalse(compareTargets(tarB, tarA1))

        val connA2 = ConnectionModelBuilder()
            .name("newname").build()

        val tarA2 = TargetModelBuilder()
            .format("jdbc")
            .tableId(123L)
            .connection(connA2)
            .build()
        Assertions.assertFalse(compareTargets(tarB, tarA2))
    }

    @Test
    fun save() {
        val formats = listOf("lake", "csv")
        val targets = formats.map {
            TestData.target(
                table!!.id!!,
                connName = "Connection-TargetTest",
                format = it
            )
        }
        val result = service.save(targets, table!!.id!!, false)
        assertThat(result).isNotNull()
        assertThat(result.isSuccess).isTrue()
        assertThat(result.storedTargets).isNotNull()
        assertThat(result.storedTargets).isNotEmpty()
        assertThat(result.storedTargets!!.mapNotNull { it.format }.sorted())
            .isEqualTo(formats.sorted())
    }

    @Test
    fun saveSameNotOverwrite() {
        val formats = listOf("lake", "csv")
        val targets = formats.map {
            TestData.target(
                table!!.id!!,
                connName = "Connection-TargetTest",
                format = it
            )
        }
        val result = service.save(targets, table!!.id!!, false)
        assertThat(result).isNotNull()
        assertThat(result!!.isSuccess).isTrue()

        val sameTargets = formats.map {
            TestData.target(
                table!!.id!!,
                connName = "Connection-TargetTest",
                format = it
            )
        }
        val result1 = service.save(sameTargets, table!!.id!!, false)
        assertThat(result1).isNotNull()
        assertThat(result1.isSuccess).isTrue()

        assertThat(result1.storedTargets).isNotNull()
        assertThat(result1.storedTargets).isNotEmpty()

        assertThat(result1.storedTargets!!.mapNotNull { it.format }.sorted())
            .isEqualTo(formats.sorted())

        for (target in result1.storedTargets!!) {
            assertThat(result!!.storedTargets!!.toList()).contains(target)
        }
    }

    @Test
    fun newTargetNotOverwrite() {
        val formats = listOf("lake", "csv")
        val targets = formats.map {
            TestData.target(
                table!!.id!!,
                connName = "Connection-TargetTest",
                format = it
            )
        }
        val result = service.save(targets, table!!.id!!, false)

        val formats1 = listOf("lake", "csv", "parquet")
        val targets1 = formats1.map {
            TestData.target(
                table!!.id!!,
                connName = "Connection-TargetTest",
                format = it
            )
        }
        val result1 = service.save(targets1, table!!.id!!, false)
        assertThat(result1).isNotNull()
        assertThat(result1!!.isSuccess).isFalse()

        val targets2 = service.getTableTargets(table!!.id!!)
        assertThat(targets2).isNotNull()
        assertThat(targets2).isNotEmpty()

        for (target in targets2) {
            assertThat(result!!.storedTargets!!.toList()).contains(target)
        }
    }

    @Test
    fun newTargetOverwrite() {
        val formats = listOf("lake", "csv")
        val targets = formats.map {
            TestData.target(
                table!!.id!!,
                connName = "Connection-TargetTest",
                format = it
            )
        }
        service.save(targets, table!!.id!!, false)

        val formats1 = listOf("lake", "csv", "parquet")
        val targets1 = formats1.map {
            TestData.target(
                table!!.id!!,
                connName = "Connection-TargetTest",
                format = it
            )
        }
        val result1 = service.save(targets1, table!!.id!!, true)
        assertThat(result1).isNotNull()
        assertThat(result1!!.isSuccess).isTrue()

        val targets2 = service.getTableTargets(table!!.id!!)
        assertThat(targets2).isNotEmpty()
        assertThat(targets2).hasSize(formats1.size)
        for (target in targets2) {
            assertThat(result1!!.storedTargets!!.toList()).contains(target)
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TargetDAOTest::class.java)

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