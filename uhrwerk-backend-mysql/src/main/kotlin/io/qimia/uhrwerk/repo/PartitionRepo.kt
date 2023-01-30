package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.builders.PartitionBuilder
import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.tools.TimeTools
import io.qimia.uhrwerk.repo.RepoUtils.jsonToMap
import io.qimia.uhrwerk.repo.RepoUtils.toJson
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime

class PartitionRepo : BaseRepo<Partition>() {
    fun save(partition: Partition): Partition? =
        super.insert(partition, INSERT) {
            insertParams(partition, it)
        }

    fun save(partitions: List<Partition>): List<Partition>? =
        partitions.map { save(it)!! }

    fun getById(id: Long): Partition? =
        super.find(
            SELECT_BY_ID, {
                it.setLong(1, id)
            }, this::map
        )

    fun getUniqueColumns(targetId: Long, ts: LocalDateTime): Partition? =
        super.find(
            SELECT_BY_UNIQUE_COLUMNS, {
                it.setLong(1, targetId)
                it.setTimestamp(2, Timestamp.valueOf(ts))
            }, this::map
        )


    fun getLatestByTargetId(targetId: Long): Partition? =
        super.find(
            SELECT_LATEST_BY_TARGET_ID,
            {
                it.setLong(1, targetId)
            }, this::map
        )

    fun getAllByTargetTs(targetId: Long, tsList: List<LocalDateTime>): List<Partition> {
        val tsStr: String = tsList.joinToString { "'${TimeTools.convertTSToUTCString(it)}'" }
        val sql = String.format(SELECT_BY_TARGET_ID_AND_TS, tsStr)

        return super.findAll(sql, { it.setLong(1, targetId) }, this::map)
    }

    fun getAllParentPartitions(partitionId: Long): List<Partition> = findAll(
        SELECT_BY_PARTDEP_PART_ID, {
            it.setLong(1, partitionId)
        }, this::map
    )


    fun deleteById(id: Long): Int? =
        super.update(DELETE_BY_ID) {
            it.setLong(1, id)
        }

    private fun insertParams(
        entity: Partition,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setLong(1, entity.targetId!!)
        insert.setTimestamp(2, Timestamp.valueOf(entity.partitionTs))
        insert.setBoolean(3, entity.partitioned)
        insert.setBoolean(4, entity.bookmarked)
        insert.setString(5, entity.maxBookmark)

        if (entity.partitionValues.isNullOrEmpty()) {
            insert.setNull(6, Types.VARCHAR)
        } else {
            insert.setString(6, toJson(entity.partitionValues!!))
        }
        return insert
    }

    private fun map(res: ResultSet): Partition {
        val builder = PartitionBuilder()
            .id(res.getLong(1))
            .targetId(res.getLong(2))
            .partitionTs(res.getTimestamp(3).toLocalDateTime())
            .partitioned(res.getBoolean(4))
            .bookmarked(res.getBoolean(5))
            .maxBookmark(res.getString(6))

        val partitionValues = res.getString(7)
        if (!partitionValues.isNullOrEmpty()){
            builder.partitionValues(jsonToMap(partitionValues))
        }

        val partitionUnit = res.getString(8)
        if (partitionUnit != null && partitionUnit.isNotEmpty())
            builder.partitionUnit(PartitionUnit.valueOf(partitionUnit))
        builder.partitionSize(res.getInt(9))
        return builder.build()
    }


    companion object {
        private val INSERT = """
            INSERT INTO PARTITION_ (target_id,
                                    partition_ts,
                                    partitioned,
                                    bookmarked,
                                    max_bookmark,
                                    partition_values)
            VALUES (?, ?, ?, ?, ?, ?)
        """.trimIndent()

        private val SELECT_BY_ID = """
            SELECT pt.id,
                   pt.target_id,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   tb.partition_unit,
                   tb.partition_size
            FROM PARTITION_ pt
                     JOIN TARGET t ON pt.target_id = t.id
                     JOIN TABLE_ tb ON t.table_id = tb.id
            WHERE pt.id = ?
        """.trimIndent()

        private val SELECT_BY_UNIQUE_COLUMNS = """
            SELECT pt.id,
                   pt.target_id,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   tb.partition_unit,
                   tb.partition_size
            FROM PARTITION_ pt
                     JOIN TARGET t ON pt.target_id = t.id
                     JOIN TABLE_ tb ON t.table_id = tb.id
            WHERE pt.target_id = ?
            AND pt.partition_ts = ?
        """.trimIndent()

        private val SELECT_LATEST_BY_TARGET_ID = """
            SELECT pt.id,
                   pt.target_id,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   tb.partition_unit,
                   tb.partition_size
            FROM PARTITION_ pt
                     JOIN TARGET t ON pt.target_id = t.id
                     JOIN TABLE_ tb ON t.table_id = tb.id
            WHERE pt.target_id = ?
            ORDER BY pt.partition_ts DESC
            LIMIT 1
        """.trimIndent()

        private val SELECT_BY_TARGET_ID_AND_TS = """
            SELECT pt.id,
                   pt.target_id,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   tb.partition_unit,
                   tb.partition_size
            FROM PARTITION_ pt
                     JOIN TARGET t ON pt.target_id = t.id
                     JOIN TABLE_ tb ON t.table_id = tb.id
            WHERE pt.target_id = ?
              AND pt.partition_ts IN (%s)
            ORDER BY pt.partition_ts ASC
        """.trimIndent()

        //get all 'parent' partitions for a partition
        private val SELECT_BY_PARTDEP_PART_ID = """
            SELECT pt.id,
                   pt.target_id,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   tb.partition_unit,
                   tb.partition_size
            FROM PARTITION_DEPENDENCY pd
                     JOIN PARTITION_ pt ON pd.dependency_partition_id = pt.id
                     JOIN TARGET t ON pt.target_id = t.id
                     JOIN TABLE_ tb ON t.table_id = tb.id
            WHERE pd.partition_id = ?
        """.trimIndent()

        private const val DELETE_BY_ID = "DELETE FROM PARTITION_ WHERE id = ?"
    }
}