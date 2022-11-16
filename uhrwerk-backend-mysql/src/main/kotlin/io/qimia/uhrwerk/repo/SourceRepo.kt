package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.SourceModel
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

class SourceRepo : BaseRepo<SourceModel>() {

    fun save(source: SourceModel): SourceModel? =
        super.insert(source, INSERT) {
            insertParams(source, it)
        }

    fun getByHashKey(hashKey: Long): SourceModel? =
        super.getByHashKey(
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
        insert.setLong(1, source.tableId!!)
        insert.setLong(2, source.connectionId!!)
        // other fields
        insert.setString(3, source.path)
        insert.setString(4, source.format)
        val sourceUnit = source.partitionUnit
        if (sourceUnit != null) {
            insert.setString(5, sourceUnit.name)
        } else {
            insert.setNull(5, Types.VARCHAR)
        }
        insert.setInt(6, source.partitionSize!!)
        insert.setString(7, source.selectQuery)
        insert.setString(8, source.parallelLoadQuery)
        insert.setString(9, source.parallelLoadColumn)
        insert.setInt(10, source.parallelLoadNum!!)
        insert.setString(11, source.selectColumn)
        insert.setBoolean(12, source.partitioned!!)
        insert.setBoolean(13, source.autoLoad!!)
        insert.setLong(14, HashKeyUtils.sourceKey(source))
        return insert
    }

    private fun map(res: ResultSet): SourceModel {
        var source = SourceModel()
        source.id = res.getLong(1)
        source.tableId = res.getLong(2)
        source.connectionId = res.getLong(3)
        source.path = res.getString(4)
        source.format = res.getString(5)

        val sourcePartitionUnit = res.getString(6)
        if (sourcePartitionUnit != null && sourcePartitionUnit != "") {
            source.partitionUnit = PartitionUnit.valueOf(sourcePartitionUnit)
        }
        source.partitionSize = res.getInt(7)
        source.selectQuery = res.getString(8)
        source.parallelLoadQuery = res.getString(9)
        source.parallelLoadColumn = res.getString(10)
        source.parallelLoadNum = res.getInt(11)
        source.selectColumn = res.getString(12)
        source.partitioned = res.getBoolean(13)
        source.autoLoad = res.getBoolean(14)
        val deactivatedTs = res.getTimestamp(15)
        if (deactivatedTs != null)
            source.deactivatedTs = deactivatedTs.toLocalDateTime()
        return source
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