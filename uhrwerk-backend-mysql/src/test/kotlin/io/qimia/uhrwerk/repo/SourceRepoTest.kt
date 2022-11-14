package io.qimia.uhrwerk.repo

import com.google.common.truth.Truth
import io.qimia.uhrwerk.TestData
import io.qimia.uhrwerk.TestHelper
import io.qimia.uhrwerk.common.model.ConnectionModel
import io.qimia.uhrwerk.common.model.HashKeyUtils
import io.qimia.uhrwerk.common.model.SourceModel
import io.qimia.uhrwerk.common.model.TableModel
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class SourceRepoTest {

    private var table: TableModel? = null
    private var connection: ConnectionModel? = null
    private var source: SourceModel? = null

    @AfterEach
    fun cleanUp() {
        TestHelper.cleanData("SOURCE", LOGGER)
        TestHelper.cleanData("TABLE_", LOGGER)
        TestHelper.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-SourceTest"))
        table = TableRepo().save(TestData.table("Table-SourceTest"))
        source = SourceRepo().save(
            TestData.source(
                "Source-SourceRepoTest",
                table!!.id,
                connection!!.id
            )
        )
    }

    @Test
    fun save() {
        Truth.assertThat(source).isNotNull()
    }

    @Test
    fun getById() {
        val source = SourceRepo().getById(source!!.id)
        Truth.assertThat(source).isNotNull()
        Truth.assertThat(source!!.path).isEqualTo("Source-SourceRepoTest")
    }

    @Test
    fun getSourcesByTableId() {
        val sources = SourceRepo().getSourcesByTableId(table!!.id)
        Truth.assertThat(sources).isNotEmpty()
        Truth.assertThat(sources.size).isEqualTo(1)
        Truth.assertThat(sources[0]).isEqualTo(source)
    }

    @Test
    fun getByHashKey() {
        val hashKey =
            HashKeyUtils.sourceKey(
                SourceModel.builder()
                    .tableId(table!!.id)
                    .connectionId(connection!!.id)
                    .path("Source-SourceRepoTest")
                    .format("jdbc").build()
            )
        val source = SourceRepo().getByHashKey(hashKey).first()
        Truth.assertThat(source).isNotNull()
        Truth.assertThat(source).isEqualTo(source)

        val hashKey1 =
            HashKeyUtils.sourceKey(SourceModel.builder()
                .tableId(table!!.id)
                .connectionId(connection!!.id)
                .path("Source-SourceRepoTest")
                .format("parquet")
                .build())

        val sources1 = SourceRepo().getByHashKey(hashKey1)
        Truth.assertThat(sources1).isEmpty()
    }

    @Test
    fun deleteById() {
        val effect = SourceRepo().deactivateById(source!!.id)
        Truth.assertThat(effect).isNotNull()
        Truth.assertThat(effect!!).isEqualTo(1)
    }

    @Test
    fun deleteByUniqueColumns() {
        /*
        val effect = SourceRepo().deleteByUniqueColumns(
            tableId!!,
            connectionId!!,
            "Source-SourceRepoTest",
            "jdbc"
        )
        Truth.assertThat(effect).isNotNull()
        Truth.assertThat(effect!!).isEqualTo(1)
         */
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(SourceRepoTest::class.java)

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