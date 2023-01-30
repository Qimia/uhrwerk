package io.qimia.uhrwerk.dao

import com.google.common.truth.Truth
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.TestData
import TestUtils
import io.qimia.uhrwerk.common.metastore.config.SourceService
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.repo.*
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class SourceDAOTest {
    val service: SourceService = SourceDAO()


    private var connection: ConnectionModel? = null

    private var table: TableModel? = null


    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("SOURCE", LOGGER)
        TestUtils.cleanData("TABLE_", LOGGER)
        TestUtils.cleanData("CONNECTION", LOGGER)
    }

    @BeforeEach
    fun addDeps() {
        connection = ConnectionRepo().save(TestData.connection("Connection-SourceDAOTest"))
        connection = ConnectionRepo().getById(connection!!.id!!)

        table = TableRepo().save(TestData.table("Table-SourceDAOTest"))
    }

    @Test
    fun save() {

        val source = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            TestData.connection("Connection-SourceDAOTest")
        )

        val result = service.save(source, true)

        LOGGER.info(result.toString())

        assertThat(result).isNotNull()

        if (result != null) {
            assertThat(result.isSuccess).isTrue()
            assertThat(result.isError).isFalse()
            assertThat(result.newResult).isNotNull()
            assertThat(result.newResult!!.id!!).isNotNull()
            assertThat(result.newResult).isEqualTo(source)
            assertThat(result.newResult!!.connection).isEqualTo(connection)
        }

    }

    @Test
    fun alreadyExistingAndDifferent() {
        val src = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!.id!!
        )

        val source = SourceRepo2().save(src)

        val src1 = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!.id!!
        )
        src1.connection = connection

        val resultSame = service.save(src1, false)
        assertThat(resultSame).isNotNull()
        if (resultSame != null) {
            assertThat(resultSame.isSuccess).isTrue()
            assertThat(resultSame.isError).isFalse()
            assertThat(resultSame.newResult).isNotNull()
            assertThat(resultSame.oldResult).isEqualTo(source)
            assertThat(resultSame.newResult).isEqualTo(src1)
        }

        val src2 = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!.id!!
        )

        src2.connection = connection
        src2.parallelPartitionColumn = "radom_column"

        val resultDifferent = service.save(src2, false)
        assertThat(resultDifferent).isNotNull()
        if (resultDifferent != null) {
            assertThat(resultDifferent.isSuccess).isFalse()
            assertThat(resultDifferent.isError).isFalse()
            assertThat(resultDifferent.newResult).isNotNull()
            assertThat(resultDifferent.newResult).isEqualTo(src2)
        }
    }

    @Test
    fun overwrite() {

        val source = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!
        )

        val result = service.save(source, true)
        assertThat(result).isNotNull()
        if (result != null) {
            assertThat(result.isSuccess).isTrue()
            assertThat(result.isError).isFalse()
            assertThat(result.newResult).isNotNull()
            assertThat(result.newResult).isEqualTo(source)
            assertThat(result.newResult!!.connection).isEqualTo(connection)
        }


        val source1 = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!
        )

        source1.autoLoad = true

        val resultChanged = service.save(source1, true)

        assertThat(resultChanged).isNotNull()
        if (resultChanged != null) {
            assertThat(resultChanged.isSuccess).isTrue()
            assertThat(resultChanged.isError).isFalse()
            assertThat(resultChanged.newResult).isNotNull()
            assertThat(resultChanged.newResult!!.id).isNotNull()
            assertThat(resultChanged.newResult).isEqualTo(source1)
            assertThat(resultChanged.oldResult).isEqualTo(source)
        }

        val source2 = SourceRepo2().getById(source.id!!)
        assertThat(source2).isNotNull()
        assertThat(source2!!.deactivatedTs).isNotNull()
    }

    @Test
    fun failWrongConnectionId() {
        val source = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        val result = service.save(source, true)
        assertThat(result).isNotNull()
        if (result != null) {
            assertThat(result.isSuccess).isFalse()
            assertThat(result.isError).isTrue()
            assertThat(result.newResult).isNotNull()
            assertThat(result.newResult!!.id).isNull()
            assertThat(result.newResult).isEqualTo(source)
        }
    }

    @Test
    fun failNoMetastoreConnection() {


        val connection1 = TestData.connection("Not-in-Metastore-Connectiom")

        val source = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        source.connection = connection1
        val result = service.save(source, true)
        assertThat(result).isNotNull()
        if (result != null) {
            assertThat(result.isSuccess).isFalse()
            assertThat(result.isError).isTrue()
            assertThat(result.newResult).isNotNull()
            assertThat(result.newResult!!.id).isNull()
            assertThat(result.newResult).isEqualTo(source)
        }
    }

    @Test
    fun withConnectionName() {
        val connection1 = TestData.connection("Connection-SourceDAOTest")

        val source = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        source.connection = connection1
        val result = service.save(source, true)
        assertThat(result).isNotNull()
        if (result != null) {
            assertThat(result.isSuccess).isTrue()
            assertThat(result.isError).isFalse()
            assertThat(result.newResult).isNotNull()
            assertThat(result.newResult!!.id).isNotNull()
            assertThat(result.newResult!!.connection).isEqualTo(connection1)
        }
    }

    @Test
    fun withConnection() {
        //source with invalid connectionId
        val source = TestData.source2(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        assertThat(source.connectionId).isNotEqualTo(connection!!.id)
        //connection instead of connectionId
        source.connection = connection
        val result = service.save(source, true)
        assertThat(result).isNotNull()
        if (result != null) {
            assertThat(result.isSuccess).isTrue()
            assertThat(result.isError).isFalse()
            assertThat(result.newResult).isNotNull()
            assertThat(result.newResult!!.id).isNotNull()
            assertThat(result.newResult!!.connection).isEqualTo(connection)
        }

    }


    companion object {

        private val LOGGER = LoggerFactory.getLogger(SourceDAOTest::class.java)

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