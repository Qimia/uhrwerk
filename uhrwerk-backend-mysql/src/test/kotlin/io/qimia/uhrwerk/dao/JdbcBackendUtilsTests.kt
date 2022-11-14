package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.model.PartitionTransformType
import io.qimia.uhrwerk.common.model.PartitionUnit
import io.qimia.uhrwerk.dao.JdbcBackendUtils.dependencyPartitions
import io.qimia.uhrwerk.dao.JdbcBackendUtils.getPartitionTs
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

class JdbcBackendUtilsTests {
    private val logger = LoggerFactory.getLogger(this.javaClass)

    @Test
    fun checkdependencyPartitions() {
        val partitions = listOf(LocalDateTime.of(2020, 8, 18, 12, 0))
        // FIXME: Doesn't pass yet
        //    LocalDateTime[][] identityPartitions = JdbcBackendUtils.dependencyPartitions(
        //            partitions,
        //            PartitionUnit.HOURS,
        //            1,
        //            PartitionUnit.MINUTES,
        //            60,
        //            PartitionTransformType.IDENTITY,
        //            1,
        //            null
        //            );
        val identityPartitions = dependencyPartitions(
            partitions,
            PartitionUnit.HOURS,
            1,
            PartitionUnit.HOURS,
            1,
            PartitionTransformType.IDENTITY,
            1
        )
        Assertions.assertEquals(partitions[0], identityPartitions[0][0])
    }

    @Test
    fun dependencyPartitionsTests() {
        val partitions = mutableListOf<LocalDateTime>()
        partitions.add(LocalDateTime.of(2020, 8, 18, 12, 0))
        partitions.add(LocalDateTime.of(2020, 8, 18, 12, 30))

        val aggPartitions = dependencyPartitions(
            partitions,
            PartitionUnit.MINUTES,
            30,
            PartitionUnit.MINUTES,
            10,
            PartitionTransformType.AGGREGATE,
            3
        )
        for ((idx: Int, partition: LocalDateTime) in partitions.withIndex()) {
            logger.info("Table-partition: " + partition)
            logger.info("Dependency-partition: " + aggPartitions.get(idx).joinToString())
        }
        val windowPartitions = dependencyPartitions(
            partitions,
            PartitionUnit.MINUTES,
            30,
            PartitionUnit.MINUTES,
            30,
            PartitionTransformType.WINDOW,
            4
        )
        for ((idx: Int, partition: LocalDateTime) in partitions.withIndex()) {
            logger.info("Table-partition: " + partition)
            logger.info("Dependency-partition: " + windowPartitions.get(idx).joinToString())
        }
    }

    @Test
    fun testGetPartitionTs() {
        val start = LocalDateTime.of(2020, 8, 24, 9, 0)
        val end = LocalDateTime.of(2020, 8, 24, 17, 0)
        val duration = Duration.of(30, ChronoUnit.MINUTES)
        val partitionTs = getPartitionTs(start, end, duration)
        for (i in partitionTs.indices) {
            logger.info(partitionTs[i].toString())
        }
    }
}