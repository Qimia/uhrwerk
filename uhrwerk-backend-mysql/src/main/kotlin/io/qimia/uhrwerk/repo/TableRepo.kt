package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.builders.TableModelBuilder
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.TableModel
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

class TableRepo() : BaseRepo<TableModel>() {

    fun save(table: TableModel): TableModel? =
        super.insert(table, INSERT) {
            insertParams(table, it)
        }

    fun getByHashKey(hashKey: Long): TableModel? =
        super.getByHashKey(
            SELECT_BY_HASH_KEY, {
                it.setLong(1, hashKey)
            }, this::map
        )

    fun getById(id: Long): TableModel? =
        super.find(SELECT_BY_ID, {
            it.setLong(1, id)
        }, this::map)

    fun getAllByTargetIds(targetIds: List<Long>): List<TableModel> {
        val idsStr = targetIds.joinToString()
        val sql = String.format(SELECT_BY_TARGET_IDS, idsStr)
        return super.findAll(sql, {}, this::map)

    }

    private fun insertParams(table: TableModel, insert: PreparedStatement): PreparedStatement {
        insert.setString(1, table.area)
        insert.setString(2, table.vertical)
        insert.setString(3, table.name)
        insert.setString(4, table.version)
        if (table.partitionUnit == null) {
            insert.setNull(5, Types.VARCHAR)
        } else {
            insert.setString(5, table.partitionUnit!!.name)
        }
        insert.setInt(6, table.partitionSize!!)
        insert.setInt(7, table.parallelism!!)
        insert.setInt(8, table.maxBulkSize!!)
        insert.setBoolean(9, table.partitioned)
        insert.setString(10, table.className)
        if (table.transformSqlQuery.isNullOrEmpty())
            insert.setNull(11, Types.VARCHAR)
        else
            insert.setString(11, table.transformSqlQuery)
        insert.setLong(12, HashKeyUtils.tableKey(table))
        return insert
    }

    fun deactivateById(id: Long): Int? =
        super.update(DEACTIVATE_BY_ID) {
            it.setLong(1, id)
        }

    private fun map(res: ResultSet): TableModel {
        val builder = TableModelBuilder()
            .id(res.getLong("tab.id"))
            .area(res.getString("tab.area"))
            .vertical(res.getString("tab.vertical"))
            .name(res.getString("tab.name"))
            .partitionSize(res.getInt("tab.partition_size"))
            .parallelism(res.getInt("tab.parallelism"))
            .maxBulkSize(res.getInt("tab.max_bulk_size"))
            .version(res.getString("tab.version"))
            .partitioned(res.getBoolean("tab.partitioned"))
            .transformSqlQuery(res.getString("tab.transform_sql_query"))
            .className(res.getString("tab.class_name"))

        val deactivatedTs = res.getTimestamp("tab.deactivated_ts")
        if (deactivatedTs != null)
            builder.deactivatedTs(deactivatedTs.toLocalDateTime())

        val partitionUnit = res.getString("partition_unit")
        if (partitionUnit != null && partitionUnit != "") {
            builder.partitionUnit(PartitionUnit.valueOf(partitionUnit))
        }

        return builder.build()
    }


    companion object {
        private val INSERT = """
            INSERT INTO TABLE_(area,
                               vertical,
                               name,
                               version,
                               partition_unit,
                               partition_size,
                               parallelism,
                               max_bulk_size,
                               partitioned,
                               class_name,
                               transform_sql_query,
                               hash_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()

        private val SELECT_BY_ID = """
            SELECT tab.id,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   tab.partition_unit,
                   tab.partition_size,
                   tab.parallelism,
                   tab.max_bulk_size,
                   tab.version,
                   tab.partitioned,
                   tab.class_name,
                   tab.transform_sql_query,
                   tab.deactivated_ts
            FROM TABLE_ tab
            WHERE id = ?
        """.trimIndent()

        private val SELECT_BY_HASH_KEY = """
            SELECT tab.id,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   tab.partition_unit,
                   tab.partition_size,
                   tab.parallelism,
                   tab.max_bulk_size,
                   tab.version,
                   tab.partitioned,
                   tab.class_name,
                   tab.transform_sql_query,
                   tab.deactivated_ts
            FROM TABLE_ tab
            WHERE tab.hash_key = ? AND tab.deactivated_ts IS NULL
        """.trimIndent()


        private val SELECT_BY_TARGET_IDS = """
            SELECT tab.id,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   tab.partition_unit,
                   tab.partition_size,
                   tab.parallelism,
                   tab.max_bulk_size,
                   tab.version,
                   tab.partitioned,
                   tab.class_name,
                   tab.transform_sql_query,
                   tab.deactivated_ts
            FROM TARGET tar
                     JOIN TABLE_ tab
                          ON tar.table_id = tab.id
            WHERE tar.id IN (%s) AND tab.deactivated_ts IS NULL
        """.trimIndent()

        private const val DEACTIVATE_BY_ID =
            "UPDATE TABLE_ SET deactivated_ts = CURRENT_TIMESTAMP() WHERE id = ?"


    }
}