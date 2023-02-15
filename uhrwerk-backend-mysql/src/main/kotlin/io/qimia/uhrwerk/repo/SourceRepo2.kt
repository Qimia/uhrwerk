package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.metastore.model.IngestionMode
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.SourceModel2
import io.qimia.uhrwerk.repo.RepoUtils.jsonToArray
import io.qimia.uhrwerk.repo.RepoUtils.toJson
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

class SourceRepo2 : BaseRepo<SourceModel2>() {

    fun save(source: SourceModel2): SourceModel2? =
        super.insert(source, INSERT) {
            insertParams(source, it)
        }

    fun getByHashKey(hashKey: Long): SourceModel2? =
        super.getByHashKey(
            SELECT_BY_HASH_KEY, {
                it.setLong(1, hashKey)
            }, this::map
        )

    fun getById(id: Long): SourceModel2? =
        super.find(SELECT_BY_ID, {
            it.setLong(1, id)
        }, this::map)

    fun getByTableId(tableId: Long): List<SourceModel2> =
        super.findAll(SELECT_ALL_BY_TABLE_ID, {
            it.setLong(1, tableId)
        }, this::map)

    fun getSourcesByTableKey(tableKey: Long): List<SourceModel2> =
        super.findAll(SELECT_ALL_BY_TABLE_KEY, {
            it.setLong(1, tableKey)
        }, this::map)

    fun deactivateByTableKey(tableKey: Long): Int? =
        super.update(DEACTIVATE_BY_TABLE_KEY) {
            it.setLong(1, tableKey)
        }

    private fun insertParams(source: SourceModel2, insert: PreparedStatement): PreparedStatement {
        insert.setLong(1, source.tableId!!)
        insert.setLong(2, source.tableKey!!)
        insert.setLong(3, source.connectionKey!!)
        insert.setString(4, source.path)
        insert.setString(5, source.format)

        val ingestionMode = source.ingestionMode
        if (ingestionMode != null) {
            insert.setString(6, ingestionMode.name)
        } else {
            insert.setNull(6, Types.VARCHAR)
        }

        val intervalTempUnit = source.intervalTempUnit
        if (intervalTempUnit != null) {
            insert.setString(7, intervalTempUnit.name)
        } else {
            insert.setNull(7, Types.VARCHAR)
        }
        insert.setInt(8, source.intervalTempSize)
        insert.setString(9, source.intervalColumn)
        insert.setString(10, source.deltaColumn)
        insert.setString(11, source.selectQuery)

        if (source.sourceVariables.isNullOrEmpty())
            insert.setNull(12, Types.VARCHAR)
        else
            insert.setString(12, toJson(source.sourceVariables!!))

        insert.setBoolean(13, source.parallelLoad)
        insert.setString(14, source.parallelPartitionQuery)
        insert.setString(15, source.parallelPartitionColumn)
        insert.setInt(16, source.parallelPartitionNum!!)
        insert.setBoolean(17, source.autoLoad)
        insert.setLong(18, HashKeyUtils.sourceKey(source))
        return insert
    }

    private fun map(res: ResultSet): SourceModel2 {
        var source = SourceModel2()
        source.id = res.getLong("id")
        source.tableId = res.getLong("table_id")
        source.tableKey = res.getLong("table_key")
        source.connectionKey = res.getLong("connection_key")
        source.path = res.getString("path")
        source.format = res.getString("format")
        val ingestionMode = res.getString("ingestion_mode")
        if (!ingestionMode.isNullOrEmpty()) {
            source.ingestionMode = IngestionMode.valueOf(ingestionMode)
        }
        val intervalTempUnit = res.getString("interval_temp_unit")
        if (!intervalTempUnit.isNullOrEmpty()) {
            source.intervalTempUnit = PartitionUnit.valueOf(intervalTempUnit)
        }
        source.intervalTempSize = res.getInt("interval_temp_size")
        source.intervalColumn = res.getString("interval_column")
        source.deltaColumn = res.getString("delta_column")
        source.selectQuery = res.getString("select_query")

        val sourceVariables = res.getString("source_variables")
        if (!sourceVariables.isNullOrEmpty()) {
            source.sourceVariables = jsonToArray(sourceVariables)
        }

        source.parallelPartitionQuery = res.getString("parallel_partition_query")
        source.parallelPartitionColumn = res.getString("parallel_partition_column")
        source.parallelPartitionNum = res.getInt("parallel_partition_num")
        source.autoLoad = res.getBoolean("auto_load")
        source.hashKey = res.getLong("hash_key")
        val deactivatedTs = res.getTimestamp("deactivated_ts")
        if (deactivatedTs != null)
            source.deactivatedTs = deactivatedTs.toLocalDateTime()
        return source
    }


    companion object {
        private val COLUMNS = listOf(
            "id",
            "table_id",
            "table_key",
            "connection_key",
            "path",
            "format",
            "ingestion_mode",
            "interval_temp_unit",
            "interval_temp_size",
            "interval_column",
            "delta_column",
            "select_query",
            "source_variables",
            "parallel_load",
            "parallel_partition_query",
            "parallel_partition_column",
            "parallel_partition_num",
            "auto_load",
            "hash_key",
            "deactivated_ts"
        )

        private val COLUMNS_STR = columnsToString(COLUMNS)

        private val INSERT = insertSql("SOURCE", COLUMNS)

        private val SELECT_BY_ID = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM SOURCE \n" +
                "WHERE id = ?"

        private val SELECT_BY_HASH_KEY = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM SOURCE \n" +
                "WHERE hash_key = ? AND deactivated_ts IS NULL"

        private val SELECT_ALL_BY_TABLE_ID = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM SOURCE \n" +
                "WHERE table_id = ?"

        private val SELECT_ALL_BY_TABLE_KEY = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM SOURCE \n" +
                "WHERE table_key = ? AND deactivated_ts IS NULL"


        private const val DEACTIVATE_BY_TABLE_KEY =
            "UPDATE SOURCE SET deactivated_ts = CURRENT_TIMESTAMP() WHERE table_key = ? AND deactivated_ts IS NULL"

    }
}