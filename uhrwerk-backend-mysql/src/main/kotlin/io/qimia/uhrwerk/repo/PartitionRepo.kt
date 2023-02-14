package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
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

    fun getLatestByTargetKey(targetKey: Long): Partition? =
        super.find(
            SELECT_LATEST_BY_TARGET_KEY,
            {
                it.setLong(1, targetKey)
            }, this::map
        )

    fun getLatestByTargetKeyPartitionValues(targetKey: Long, partsJson: String): List<Partition> =
        super.findAll(
            SELECT_BY_TARGET_KEY_PART_VALUES,
            {
                it.setLong(1, targetKey)
                it.setString(2, partsJson)
            }, this::map
        )

    fun getAllByTargetTs(targetId: Long, tsList: List<LocalDateTime>): List<Partition> {
        val tsStr: String = tsList.joinToString { "'${RepoUtils.convertTSToUTCString(it)}'" }
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
        insert.setLong(1, entity.tableKey!!)
        insert.setLong(2, entity.targetKey!!)
        insert.setTimestamp(3, Timestamp.valueOf(entity.partitionTs))
        insert.setBoolean(4, entity.partitioned)
        insert.setBoolean(5, entity.bookmarked)
        insert.setString(6, entity.maxBookmark)

        if (entity.partitionValues.isNullOrEmpty()) {
            insert.setNull(7, Types.VARCHAR)
        } else {
            insert.setString(7, toJson(entity.partitionValues!!))
        }
        if (entity.partitionPath.isNullOrEmpty()) {
            insert.setNull(8, Types.VARCHAR)
        } else {
            insert.setString(8, entity.partitionPath)
        }
        return insert
    }

    private fun map(res: ResultSet): Partition {
        val partition = Partition()
        partition.id = res.getLong("pt.id")
        partition.targetKey = res.getLong("pt.target_key")
        partition.tableKey = res.getLong("pt.table_key")
        partition.partitionTs = res.getTimestamp("pt.partition_ts").toLocalDateTime()
        partition.partitioned = res.getBoolean("pt.partitioned")
        partition.bookmarked = res.getBoolean("pt.bookmarked")
        partition.maxBookmark = res.getString("pt.max_bookmark")

        val partitionValues = res.getString("pt.partition_values")
        if (!partitionValues.isNullOrEmpty()) {
            partition.partitionValues = jsonToMap(partitionValues)
        }
        partition.partitionPath = res.getString("pt.partition_path")

        val partitionUnit = res.getString("tb.partition_unit")
        if (!partitionUnit.isNullOrEmpty())
            partition.partitionUnit = PartitionUnit.valueOf(partitionUnit)

        partition.partitionSize = res.getInt("tb.partition_size")

        return partition
    }


    companion object {
        private val INSERT = """
            INSERT INTO PARTITION_ (table_key,
                                    target_key,
                                    partition_ts,
                                    partitioned,
                                    bookmarked,
                                    max_bookmark,
                                    partition_values,
                                    partition_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()

        private val SELECT_BY_ID = """
            SELECT pt.id,
                   pt.target_key,
                   pt.table_key,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   pt.partition_path,
                   tb.partition_unit,
                   tb.partition_size
            FROM PARTITION_ pt
                     JOIN TARGET t ON pt.target_id = t.id
                     JOIN TABLE_ tb ON t.table_id = tb.id
            WHERE pt.id = ?
        """.trimIndent()

        private val SELECT_LATEST_BY_TARGET_KEY = """
            SELECT pt.id,
                   pt.target_key,
                   pt.table_key,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   pt.partition_path,
                   tb.partition_unit,
                   tb.partition_size
            FROM PARTITION_ pt
                     JOIN TARGET t ON pt.target_key = t.hashed_key
                     JOIN TABLE_ tb ON t.table_key = tb.hashed_key
            WHERE pt.target_key = ? AND pt.deactivated_ts IS NULL
            ORDER BY pt.partition_ts DESC
            LIMIT 1
        """.trimIndent()

        private val SELECT_BY_TARGET_KEY_PART_VALUES = """ 
            SELECT pt.id,
               pt.target_id,
               pt.partition_ts,
               pt.partitioned,
               pt.bookmarked,
               pt.max_bookmark,
               pt.partition_values,
               pt.partition_path,
               tb.partition_unit,
               tb.partition_size
            FROM (SELECT partition_values,
                         MAX(id)           as last_partition_id,
                         MAX(partition_ts) as last_partition_ts
                FROM PARTITION_
                WHERE target_key = ?
                    AND JSON_CONTAINS(partition_values, ?)
                    AND deactivated_ts IS NULL
                GROUP BY partition_values) tmp
                     JOIN PARTITION_ pt ON pt.id = last_partition_id
                     JOIN TARGET t ON pt.target_key = t.hash_key
                     JOIN TABLE_ tb ON t.table_key = tb.hash_key 
         """.trimIndent()


        private val SELECT_BY_TARGET_ID_AND_TS = """
            SELECT pt.id,
                   pt.target_id,
                   pt.partition_ts,
                   pt.partitioned,
                   pt.bookmarked,
                   pt.max_bookmark,
                   pt.partition_values,
                   pt.partition_path,
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
                   pt.partition_path,
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