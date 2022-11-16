package io.qimia.uhrwerk.dao

import com.google.common.truth.Truth
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.TestData
import TestUtils
import io.qimia.uhrwerk.common.metastore.config.SourceService
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.repo.ConnectionRepo
import io.qimia.uhrwerk.repo.HikariCPDataSource
import io.qimia.uhrwerk.repo.SourceRepo
import io.qimia.uhrwerk.repo.TableRepo
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

        val source = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            TestData.connection("Connection-SourceDAOTest")
        )

        val result = service.save(source, table, true)

        LOGGER.info(result.toString())

        Truth.assertThat(result).isNotNull()
        Truth.assertThat(result.isSuccess).isTrue()
        Truth.assertThat(result.isError).isFalse()
        Truth.assertThat(result.newResult).isNotNull()
        Truth.assertThat(result.newResult.id).isNotNull()
        Truth.assertThat(result.newResult).isEqualTo(source)
        Truth.assertThat(result.newResult.connection).isEqualTo(connection)
    }

    @Test
    fun alreadyExistingAndDifferent() {
        val src = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!.id!!
        )

        val source = SourceRepo().save(src)

        val src1 = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!.id!!
        )
        src1.connection = connection

        val resultSame = service.save(src1, table, false)
        Assertions.assertTrue(resultSame.isSuccess)
        Assertions.assertFalse(resultSame.isError)
        Assertions.assertNotNull(resultSame.newResult)

        Assertions.assertEquals(resultSame.oldResult, source)
        Assertions.assertEquals(src1, resultSame.oldResult)
        Assertions.assertEquals(src1, resultSame.newResult)


        val src2 = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!.id!!
        )

        src2.connection = connection
        src2.partitionUnit = PartitionUnit.HOURS
        src2.partitionSize = 200

        val resultDifferent = service.save(src2, table, false)
        Assertions.assertFalse(resultDifferent.isError)
        Assertions.assertFalse(resultDifferent.isSuccess)
        Assertions.assertNotNull(resultDifferent.newResult)
        Assertions.assertEquals(src2, resultDifferent.newResult)
    }

    @Test
    fun overwrite() {

        val source = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!
        )

        val result = service.save(source, table, true)

        Assertions.assertTrue(result.isSuccess)
        Assertions.assertFalse(result.isError)
        Assertions.assertEquals(source, result.newResult)

        val source1 = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            connection!!
        )

        source1.partitionSize = 5
        source1.selectColumn = "column"
        source1.partitionUnit = PartitionUnit.HOURS
        source1.parallelLoadNum = 20
        source1.autoLoad = true

        val resultChanged = service.save(source1, table, true)

        Assertions.assertTrue(resultChanged.isSuccess)
        Assertions.assertFalse(resultChanged.isError)
        Assertions.assertNotNull(resultChanged.newResult.id)



        assertThat(source).isEqualTo(resultChanged.oldResult)


        println(source1)
        println(resultChanged.newResult)

        assertThat(source1).isEqualTo(resultChanged.newResult)

        val source2 = SourceRepo().getById(source.id!!)
        Truth.assertThat(source2!!.deactivatedTs).isNotNull()
    }

    @Test
    fun failWrongConnectionId() {
        val source = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        val result = service.save(source, table, true)

        Assertions.assertFalse(result.isSuccess)
        Assertions.assertTrue(result.isError)
        Assertions.assertNotNull(result.newResult)
        Assertions.assertNull(result.newResult.id)
        Assertions.assertEquals(source, result.newResult)
    }

    @Test
    fun failNoMetastoreConnection() {


        val connection1 = TestData.connection("Not-in-Metastore-Connectiom")

        val source = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        source.connection = connection1
        val result = service.save(source, table, true)

        Assertions.assertFalse(result.isSuccess)
        Assertions.assertTrue(result.isError)
        Assertions.assertNotNull(result.newResult)
        Assertions.assertNull(result.newResult.id)
        Assertions.assertEquals(source, result.newResult)
    }

    @Test
    fun withConnectionName() {
        val connection1 = TestData.connection("Connection-SourceDAOTest")

        val source = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        source.connection = connection1
        val result = service.save(source, table, true)

        Assertions.assertTrue(result.isSuccess)
        Assertions.assertFalse(result.isError)
        Assertions.assertNotNull(result.newResult)
        Assertions.assertNotNull(result.newResult.id)
        Truth.assertThat(result.newResult.connectionId).isEqualTo(connection!!.id)
    }

    @Test
    fun withConnection() {
        //source with invalid connectionId
        val source = TestData.source(
            "Source-SourceDAOTest",
            table!!.id!!,
            -100
        )
        Truth.assertThat(source.connectionId).isNotEqualTo(connection!!.id)
        //connection instead of connectionId
        source.connection = connection
        val result = service.save(source, table, true)

        Assertions.assertFalse(result.isError)
        Assertions.assertTrue(result.isSuccess)
        Assertions.assertNotNull(result.newResult)
        Assertions.assertNotNull(result.newResult.id)
        Truth.assertThat(result.newResult.connectionId).isEqualTo(connection!!.id)
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