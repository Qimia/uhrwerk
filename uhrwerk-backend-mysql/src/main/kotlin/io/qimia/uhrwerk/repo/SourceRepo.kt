package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.model.HashKeyUtils
import io.qimia.uhrwerk.common.model.PartitionUnit
import io.qimia.uhrwerk.common.model.SourceModel
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

class SourceRepo : BaseRepo<SourceModel>() {

    fun save(source: SourceModel): SourceModel? =
        super.insert(source,INSERT) {
            insertParams(source, it)
        }

    fun getByHashKey(hashKey: Long): List<SourceModel> =
        super.findAll(
            SELECT_BY_HASH_KEY, {
                it.setLong(1, hashKey)
            }, this::map
        )

    fun getById(id: Long): SourceModel? =
        super.find(SELECT_BY_ID, {
            it.setLong(1, id)
        }, this::map)

    fun getSourcesByTableId(tableId: Long): List<SourceModel> =
        super.findAll(SELECT_ALL_BY_TABLE_ID, {
            it.setLong(1, tableId)
        }, this::map)

    fun deactivateById(id: Long): Int? =
        super.update(DEACTIVATE_BY_ID) {
            it.setLong(1, id)
        }

    private fun insertParams(source: SourceModel, insert: PreparedStatement): PreparedStatement {
        insert.setLong(1, source.tableId)
        insert.setLong(2, source.connectionId)
        // other fields
        insert.setString(3, source.path)
        insert.setString(4, source.format)
        val sourceUnit = source.partitionUnit
        if (sourceUnit != null) {
            insert.setString(5, sourceUnit.name)
        } else {
            insert.setNull(5, Types.VARCHAR)
        }
        insert.setInt(6, source.partitionSize)
        insert.setString(7, source.selectQuery)
        insert.setString(8, source.parallelLoadQuery)
        insert.setString(9, source.parallelLoadColumn)
        insert.setInt(10, source.parallelLoadNum)
        insert.setString(11, source.selectColumn)
        insert.setBoolean(12, source.isPartitioned)
        insert.setBoolean(13, source.isAutoLoad)
        insert.setLong(14, HashKeyUtils.sourceKey(source))
        return insert
    }

    private fun map(res: ResultSet): SourceModel {
        var builder = SourceModel.builder()
            .id(res.getLong(1))
            .tableId(res.getLong(2))
            .connectionId(res.getLong(3))
            .path(res.getString(4))
            .format(res.getString(5))

        val sourcePartitionUnit = res.getString(6)
        if (sourcePartitionUnit != null && sourcePartitionUnit != "") {
            builder.partitionUnit(PartitionUnit.valueOf(sourcePartitionUnit))
        }

        builder.partitionSize(res.getInt(7))
            .selectQuery(res.getString(8))
            .parallelLoadQuery(res.getString(9))
            .parallelLoadColumn(res.getString(10))
            .parallelLoadNum(res.getInt(11))
            .selectColumn(res.getString(12))
            .partitioned(res.getBoolean(13))
            .autoLoad(res.getBoolean(14))

        val deactivatedTs = res.getTimestamp(15)
        if (deactivatedTs != null)
            builder.deactivatedTs(deactivatedTs.toLocalDateTime())

        return builder.build()
    }

    companion object {
        private val INSERT = """
            INSERT INTO SOURCE(table_id,
                               connection_id,
                               path,
                               format,
                               partition_unit,
                               partition_size,
                               sql_select_query,
                               sql_partition_query,
                               partition_column,
                               partition_num,
                               query_column,
                               partitioned,
                               auto_load,
                               hash_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
        """.trimIndent()

        private val SELECT_BY_ID = """
            SELECT id,
                   table_id,
                   connection_id,
                   path,
                   format,
                   partition_unit,
                   partition_size,
                   sql_select_query,
                   sql_partition_query,
                   partition_column,
                   partition_num,
                   query_column,
                   partitioned,
                   auto_load,
                   deactivated_ts
            FROM SOURCE
            WHERE id = ?
        """.trimIndent()

        private val SELECT_BY_HASH_KEY = """
            SELECT id,
                   table_id,
                   connection_id,
                   path,
                   format,
                   partition_unit,
                   partition_size,
                   sql_select_query,
                   sql_partition_query,
                   partition_column,
                   partition_num,
                   query_column,
                   partitioned,
                   auto_load,
                   deactivated_ts
            FROM SOURCE
            WHERE hash_key = ? AND deactivated_ts IS NULL
        """.trimIndent()

        private val SELECT_ALL_BY_TABLE_ID = """
            SELECT id,
                   table_id,
                   connection_id,
                   path,
                   format,
                   partition_unit,
                   partition_size,
                   sql_select_query,
                   sql_partition_query,
                   partition_column,
                   partition_num,
                   query_column,
                   partitioned,
                   auto_load,
                   deactivated_ts
            FROM SOURCE
            WHERE table_id = ? AND deactivated_ts IS NULL
        """.trimIndent()

        private const val DEACTIVATE_BY_ID =
            "UPDATE SOURCE SET deactivated_ts = CURRENT_TIMESTAMP() WHERE id = ?"

    }
}