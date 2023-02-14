package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.ConnectionHelper
import io.qimia.uhrwerk.common.metastore.model.Partition
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.time.LocalDateTime

class PartitionTest {

    @BeforeEach
    fun before(){
        LOGGER.info("BEFORE_EACH: Hello will run a TEST!!!!")

    }

    @Test
    internal fun create() {
        val partition = Partition()
        partition.targetKey = 300L
        partition.partitionTs = LocalDateTime.now()
        partition.partitioned = false
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PartitionTest::class.java)
        lateinit var db: Connection
        lateinit var partitionDao: PartitionDAO

        @BeforeAll
        @JvmStatic
        internal fun setUp() {
            db = ConnectionHelper.getConnecion()
            partitionDao = PartitionDAO()
        }

        @AfterAll
        @JvmStatic
        internal fun afterAll() {
            LOGGER.info("afterAll called")
        }
    }
}