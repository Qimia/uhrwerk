package io.qimia.uhrwerk.integration

import TestUtils
import TestUtils.filePath
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import io.qimia.uhrwerk.dao.FunctionDefinitionDAO
import io.qimia.uhrwerk.repo.HikariCPDataSource
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class FunctionDefinitionIntTest {

    private val service = FunctionDefinitionDAO()

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("FUNCTION_DEFINITION", LOGGER)
    }

    @BeforeEach
    fun saveData() {
    }

    @Test
    fun read() {
        val functionDefs = YamlConfigReader().readFunctionDefinitions(YAML_FILE)
        assertThat(functionDefs).isNotNull()
        assertThat(functionDefs!!.toList()).hasSize(2)
        val classFunc1 = functionDefs!![0]
        assertThat(classFunc1).isNotNull()
        assertThat(classFunc1.name).isNotNull()
        assertThat(classFunc1.name).isEqualTo("my_lookup_class_func")
        assertThat(classFunc1.className).isNotNull()
        assertThat(classFunc1.className).isEqualTo("com.mycompany.MyLookupFunction")
        assertThat(classFunc1.params).isNotNull()
        assertThat(classFunc1.params).asList().hasSize(2)
        assertThat(classFunc1.params).isEqualTo(arrayOf("first_param", "second_param"))

        assertThat(classFunc1.inputViews).isNotNull()
        assertThat(classFunc1.inputViews).asList().hasSize(2)
        assertThat(classFunc1.inputViews).isEqualTo(arrayOf("table_a", "table_b"))

        assertThat(classFunc1.output).isNotNull()
        assertThat(classFunc1.output).isEqualTo("my_lookup_output_view")

        val sqlFunc1 = functionDefs!![1]
        assertThat(sqlFunc1).isNotNull()
        assertThat(sqlFunc1.name).isNotNull()
        assertThat(sqlFunc1.name).isEqualTo("my_lookup_sql_func")
        assertThat(sqlFunc1.className).isNull()
        assertThat(sqlFunc1.params).isNotNull()
        assertThat(sqlFunc1.params).asList().hasSize(3)
        assertThat(sqlFunc1.params).isEqualTo(arrayOf("first_param", "second_param", "third_param"))
        assertThat(sqlFunc1.inputViews).isNotNull()
        assertThat(sqlFunc1.inputViews).asList().hasSize(1)
        assertThat(sqlFunc1.inputViews).isEqualTo(arrayOf("category_table"))
        assertThat(sqlFunc1.output).isNotNull()
        assertThat(sqlFunc1.output).isEqualTo("my_lookup_sql_output_view")




    }


    @Test
    fun insertFullConnection() {
        val functionDefs = YamlConfigReader().readFunctionDefinitions(YAML_FILE)
        val results = functionDefs?.map { service.save(it, true)!! }
        functionDefs?.forEach { assertThat(it!!.id).isNotNull() }

        results?.forEach {
            assertThat(it).isNotNull()
            assertThat(it.isSuccess).isTrue()
            assertThat(it.isError).isFalse()
            assertThat(it.newFunctionDefinition).isNotNull()
            assertThat(it.oldFunctionDefinition).isNull()
        }

        val resultFunctionDefs = results?.filter { it.isSuccess }?.map { it.newFunctionDefinition }
        assertThat(resultFunctionDefs).isNotEmpty()
        assertThat(resultFunctionDefs?.size).isEqualTo(functionDefs?.size)
        assertThat(resultFunctionDefs?.get(0)).isEqualTo(functionDefs?.get(0))

        val dbFunctionClass = service.getByHashKey(HashKeyUtils.functionKey("my_lookup_class_func"))
        assertThat(dbFunctionClass).isNotNull()
        assertThat(dbFunctionClass!!.id).isEqualTo(resultFunctionDefs?.get(0)?.id)
        assertThat(dbFunctionClass).isEqualTo(functionDefs?.get(0))

        LOGGER.info("CLASS FunctionDefinition: {}", functionDefs?.get(0))
        LOGGER.info("CLASS DB FunctionDefinition: {}", dbFunctionClass)

        val dbFunctionSQL = service.getByHashKey(HashKeyUtils.functionKey("my_lookup_sql_func"))
        assertThat(dbFunctionSQL).isNotNull()
        assertThat(dbFunctionSQL!!.id).isEqualTo(resultFunctionDefs?.get(1)?.id)
        assertThat(dbFunctionSQL).isEqualTo(functionDefs?.get(1))

        LOGGER.info("SQL FunctionDefinition: {}", functionDefs?.get(1))
        LOGGER.info("SQL DB FunctionDefinition: {}", dbFunctionSQL)

    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(FunctionDefinitionIntTest::class.java)

        @Container
        var MY_SQL_DB: MySQLContainer<*> = TestUtils.mysqlContainer()

        private const val FUNC_DEF_YAML = "config/function-definition.yml"
        private val YAML_FILE = filePath(FUNC_DEF_YAML)!!

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