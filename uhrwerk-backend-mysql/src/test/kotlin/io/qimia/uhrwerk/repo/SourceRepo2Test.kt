package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import TestUtils
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.common.metastore.builders.SourceModelBuilder
import io.qimia.uhrwerk.common.metastore.model.*
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class SourceRepo2Test {

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var source: SourceModel2? = null

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("SOURCE", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-SourceTest"))
        table = TableRepo().save(TestData.table("Table-SourceTest"))
        source = SourceRepo2().save(
            TestData.source2(
                "Source-SourceRepoTest",
                table!!.id!!,
                connection!!.id!!
            )
        )
    }

    @Test
    fun save() {
        assertThat(source).isNotNull()
    }

    @Test
    fun getById() {
        val source = SourceRepo2().getById(source!!.id!!)
        assertThat(source).isNotNull()
        assertThat(source!!.path).isEqualTo("Source-SourceRepoTest")
        assertThat(source.sourceVariables).isNotNull()
        assertThat(source.sourceVariables!!.size).isEqualTo(2)
        assertThat(source.sourceVariables!!).isEqualTo(arrayOf("var1", "var2"))

    }

    @Test
    fun getSourcesByTableId() {
        val sources = SourceRepo2().getSourcesByTableId(table!!.id!!)
        assertThat(sources).isNotEmpty()
        assertThat(sources.size).isEqualTo(1)
        assertThat(sources[0]).isEqualTo(source)
    }

    @Test
    fun getByHashKey() {
        val hashKey =
            HashKeyUtils.sourceKey(
                SourceModelBuilder()
                    .tableId(table!!.id!!)
                    .connectionId(connection!!.id!!)
                    .path("Source-SourceRepoTest")
                    .format("jdbc").build()
            )
        val source = SourceRepo2().getByHashKey(hashKey)
        assertThat(source).isNotNull()
        assertThat(source).isEqualTo(source)

        val hashKey1 =
            HashKeyUtils.sourceKey(
                SourceModelBuilder()
                    .tableId(table!!.id!!)
                    .connectionId(connection!!.id!!)
                    .path("Source-SourceRepoTest")
                    .format("parquet")
                    .build()
            )

        val source1 = SourceRepo2().getByHashKey(hashKey1)
        assertThat(source1).isNull()
    }

    @Test
    fun deleteById() {
        val effect = SourceRepo2().deactivateById(source!!.id!!)
        assertThat(effect).isNotNull()
        assertThat(effect!!).isEqualTo(1)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(SourceRepo2Test::class.java)

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