package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import java.sql.PreparedStatement
import java.sql.SQLException
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

object JdbcBackendUtils {
    @Throws(SQLException::class)
    fun singleRowUpdate(statement: PreparedStatement): Long? {
        statement.executeUpdate()
        val generatedKeys = statement.generatedKeys
        return if (generatedKeys.next()) generatedKeys.getLong(1) else null
    }

    @JvmStatic
    fun dependencyPartitions(
        tablePartitions: List<LocalDateTime>,
        tableUnit: PartitionUnit,
        tableSize: Int,
        depTableUnit: PartitionUnit?,
        depTableSize: Int,
        transformType: PartitionTransformType,
        transformSize: Int): List<List<LocalDateTime>> {
        if (transformType == PartitionTransformType.IDENTITY) {
            assert(tableUnit == depTableUnit && tableSize == depTableSize)
            return tablePartitions.map { listOf(it) }
        }

        if (transformType == PartitionTransformType.AGGREGATE) {
            val tableChronoUnit = ChronoUnit.valueOf(tableUnit.name)
            val depTableChronoUnit = ChronoUnit.valueOf(depTableUnit!!.name)
            val tableDuration = Duration.of(tableSize.toLong(), tableChronoUnit)
            val depDuration = Duration.of(depTableSize.toLong(), depTableChronoUnit)
            val transformDuration = Duration.of((transformSize * depTableSize).toLong(), depTableChronoUnit)
            assert(tableDuration == transformDuration)
            return tablePartitions.map { ts -> (0L until transformSize).map { ts.plus(depDuration.multipliedBy(it)) } }
        }
        if (transformType == PartitionTransformType.WINDOW) {
            assert(tableUnit == depTableUnit && tableSize == depTableSize)
            val depTableChronoUnit = ChronoUnit.valueOf(depTableUnit!!.name)
            val depDuration = Duration.of(depTableSize.toLong(), depTableChronoUnit)
            return tablePartitions.map { ts -> (transformSize downTo 0L).map { ts.minus(depDuration.multipliedBy(it)) } }
        }
        return emptyList()
    }

    @JvmStatic
    fun getPartitionTs(
            start: LocalDateTime, end: LocalDateTime, duration: Duration): List<LocalDateTime> {
        val divided = Duration.between(start, end).dividedBy(duration).toLong()
        return (0L until divided).map { start.plus(duration.multipliedBy(it.toLong())) }
    }
}