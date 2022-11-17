package io.qimia.uhrwerk.repo

import TestUtils
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.TestData
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.metastore.model.SecretModel
import org.junit.jupiter.api.*
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal class SecretRepoTest {

    private val repo = SecretRepo()

    private var secret: SecretModel? = null

    @AfterEach
    fun cleanUp() {
        TestUtils.cleanData("SECRET_", LOGGER)
    }

    @BeforeEach
    fun saveData() {
        val scr = TestData.secret("meta_store_db_user")
        secret = repo.save(scr)
    }


    @Test
    fun save() {
        assertThat(secret).isNotNull()
        assertThat(secret!!.id).isNotNull()
    }

    @Test
    fun getById() {
        val scr = repo.getById(secret!!.id!!)
        assertThat(scr).isNotNull()
        assertThat(scr!!.name).isEqualTo("meta_store_db_user")
    }

    @Test
    fun getByHashKey() {
        val scr = SecretModel()
        scr.name = "meta_store_db_user"

        val hashKey = HashKeyUtils.secretKey(scr)
        val secret1 = repo.getByHashKey(hashKey)

        assertThat(secret1).isNotNull()
        assertThat(secret1!!.name).isEqualTo("meta_store_db_user")
        assertThat(secret1!!).isEqualTo(secret)
    }


    @Test
    fun deactivateById() {
        val effect = repo.deactivateById(secret!!.id!!)
        assertThat(effect).isNotNull()
        assertThat(effect!!).isEqualTo(1)

        val scr = SecretModel()
        scr.name = "meta_store_db_user"

        val hashKey = HashKeyUtils.secretKey(scr)

        val notActive = repo.getByHashKey(hashKey)

        assertThat(notActive).isNull()

        val notActive1 = repo.getById(secret!!.id!!)

        assertThat(notActive1).isNotNull()
        assertThat(notActive1!!.deactivatedTs).isNotNull()

        var sameName: SecretModel? = TestData.secret("meta_store_db_user")
        sameName!!.awsRegion = "eu_cent_1"

        sameName = repo.save(sameName)
        assertThat(sameName).isNotNull()
        assertThat(sameName).isNotEqualTo(secret)

        val curActive = repo.getByHashKey(hashKey)
        assertThat(curActive?.name).isEqualTo("meta_store_db_user")
        assertThat(curActive?.deactivatedTs).isNull()

    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(SecretRepoTest::class.java)

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