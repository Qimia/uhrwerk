package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.TargetModel
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class TargetRepoTest {

    private val repo: TargetRepo = TargetRepo()

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var target: TargetModel? = null

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("TARGET", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-TargetTest"))
        table = TableRepo().save(TestData.table("Table-TargetTest"))
        target = repo.save(TestData.target(connection!!.id!!, table!!.id!!))
    }

    @Test
    fun save() {
        val formats = listOf("lake", "csv")
        val targets = formats.map { TestData.target(table!!.id!!, connection!!.id!!, it) }
        val targetIds = repo.save(targets)
        Truth.assertThat(targetIds).isNotNull()
        Truth.assertThat(targetIds).isNotEmpty()
        Truth.assertThat(targetIds).hasSize(2)

        for (tar in targetIds!!) {
            val target = repo.getById(tar.id!!)
            Truth.assertThat(target).isNotNull()
            Truth.assertThat(target!!.tableId).isEqualTo(table!!.id!!)
            Truth.assertThat(target!!.connectionId).isEqualTo(connection!!.id!!)
            Truth.assertThat(target!!.format).isIn(formats)
        }
    }

    @Test
    fun saveBatch() {

        Truth.assertThat(target!!.id!!).isNotNull()
    }

    @Test
    fun getById() {
        val target1 = repo.getById(target!!.id!!)
        Truth.assertThat(target1).isNotNull()
        Truth.assertThat(target1!!.tableId).isEqualTo(table!!.id!!)
        Truth.assertThat(target1!!.connectionId).isEqualTo(connection!!.id!!)
    }

    @Test
    fun deactivate() {

        val targets = repo.getByTableId(table!!.id!!)
        Truth.assertThat(targets).isNotEmpty()
        Truth.assertThat(targets).isNotEmpty()
        Truth.assertThat(targets[0].deactivatedTs).isNull()

        val effect = repo.deactivateByTableId(table!!.id!!)
        Truth.assertThat(effect).isNotNull()
        Truth.assertThat(effect!!).isEqualTo(targets.size)

        val target1 = repo.getById(target!!.id!!)
        Truth.assertThat(target1).isNotNull()
        Truth.assertThat(target1!!.deactivatedTs).isNotNull()

        repo.save(TestData.target(connection!!.id!!, table!!.id!!))

    }

    @Test
    fun deactivateOverwrite() {

        val targets = repo.getByTableId(table!!.id!!)

        val effect = repo.deactivateByTableId(table!!.id!!)
        Truth.assertThat(effect).isNotNull()
        Truth.assertThat(effect!!).isEqualTo(targets.size)

        val target1 = repo.save(TestData.target(table!!.id!!, connection!!.id!!))
        Truth.assertThat(target1).isNotNull()

        val targets1 = repo.getByTableId(table!!.id!!)
        Truth.assertThat(targets1).isNotEmpty()
        Truth.assertThat(targets1).hasSize(targets.size)
        Truth.assertThat(targets1[0].id).isNotEqualTo(targets[0].id)
        Truth.assertThat(targets1[0].tableId).isEqualTo(targets[0].tableId)
        Truth.assertThat(targets1[0].connectionId).isEqualTo(targets[0].connectionId)

        val deactiveTarget = repo.getById(target!!.id!!)
        Truth.assertThat(deactiveTarget).isNotNull()
        Truth.assertThat(deactiveTarget!!.deactivatedTs).isNotNull()

    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(TargetRepoTest::class.java)

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