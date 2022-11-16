package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class TableRepoTest {

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("TABLE_", LOGGER)
    }

    @Test
    fun save() {
        val repo = TableRepo()
        val table = TestData.table("saveTestTable")
        val tableId = repo.save(table)
        Truth.assertThat(tableId).isNotNull()
    }

    @Test
    fun getById() {
        val repo = TableRepo()
        val table = TestData.table("getByIdTestTable")
        val table1 = repo.save(table)

        Truth.assertThat(table1).isNotNull()
        val table2 = repo.getById(table1!!.id!!)

        Truth.assertThat(table2).isNotNull()
        Truth.assertThat(table2!!.name).isEqualTo("getByIdTestTable")

    }

    @Test
    fun deactivateById() {
        val repo = TableRepo()
        val table = TestData.table("deleteByIdTestTable")
        val table1 = repo.save(table)

        Truth.assertThat(table1).isNotNull()
        val effect = repo.deactivateById(table1!!.id!!)

        Truth.assertThat(effect).isNotNull()
        Truth.assertThat(effect!!).isEqualTo(1)

    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(TableRepoTest::class.java)

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