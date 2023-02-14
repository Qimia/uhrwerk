package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.model.TargetModel
import java.sql.PreparedStatement
import java.sql.ResultSet

class TargetRepo : BaseRepo<TargetModel>() {
    fun save(target: TargetModel): TargetModel? =
        super.insert(target, INSERT) {
            insertParams(target, it)
        }

    fun save(targets: List<TargetModel>): List<TargetModel>? =
        targets.map { save(it)!! }

    fun getByHashKey(hashKey: Long): TargetModel? =
        super.getByHashKey(SELECT_BY_HASH_KEY, {
            it.setLong(1, hashKey)
        }, this::map)


    fun getById(id: Long): TargetModel? =
        super.find(SELECT_BY_ID, {
            it.setLong(1, id)
        }, this::map)

    fun getByTableId(tableId: Long): List<TargetModel> =
        super.findAll(SELECT_BY_TABLE_ID, {
            it.setLong(1, tableId)
        }, this::map)

    fun getByTableKey(tableKey: Long): List<TargetModel> =
        super.findAll(SELECT_BY_TABLE_KEY, {
            it.setLong(1, tableKey)
        }, this::map)

    fun getByTableKeyFormat(tableKey: Long, format: String): List<TargetModel> =
        super.findAll(SELECT_BY_TABLE_KEY_FORMAT, {
            it.setLong(1, tableKey)
            it.setString(2, format)
        }, this::map)

    fun deactivateByTableKey(tableKey: Long): Int? =
        super.update(DEACTIVATE_BY_TABLE_KEY) {
            it.setLong(1, tableKey)
        }

    private fun insertParams(entity: TargetModel, insert: PreparedStatement): PreparedStatement {
        insert.setLong(1, entity.tableId!!)
        insert.setLong(2, entity.tableKey!!)
        insert.setLong(3, entity.connectionKey!!)
        insert.setString(4, entity.format)
        insert.setString(5, entity.tableName)
        insert.setLong(6, HashKeyUtils.targetKey(entity))
        return insert
    }

    private fun map(res: ResultSet): TargetModel {
        val target = TargetModel()
        target.id = res.getLong(1)
        target.tableId = res.getLong(2)
        target.tableKey = res.getLong(3)
        target.connectionKey = res.getLong(4)
        target.format = res.getString(5)
        target.tableName = res.getString(6)
        target.hashKey = res.getLong(7)
        target.deactivatedTs = res.getTimestamp(8)?.toLocalDateTime()
        return target
    }


    companion object {
        private const val INSERT =
            "INSERT INTO TARGET (table_id, table_key, connection_key, format, table_name, hash_key) VALUES (?, ?, ?, ?, ?, ?)"

        private const val SELECT_BY_ID =
            "SELECT id, table_id, table_key, connection_key, format, table_name, hash_key, deactivated_ts FROM TARGET WHERE id = ?"

        private const val SELECT_BY_HASH_KEY =
            "SELECT id, table_id, table_key, connection_key, format, table_name, hash_key, deactivated_ts FROM TARGET WHERE hash_key = ? AND deactivated_ts IS NULL"


        private const val SELECT_BY_TABLE_ID =
            "SELECT id, table_id, table_key, connection_key, format, table_name, hash_key, deactivated_ts FROM TARGET WHERE table_id = ?"

        private val SELECT_BY_TABLE_KEY = """
            SELECT tar.id,
               tar.table_id,
               tar.table_key,
               tar.connection_key,
               tar.format,
               tar.table_name,
               tar.hash_key,
               tar.deactivated_ts
            FROM TARGET tar
            WHERE tar.table_key = ?
              AND tar.deactivated_ts IS NULL
        """.trimIndent()

        private val SELECT_BY_TABLE_KEY_FORMAT = """
            SELECT tar.id,
               tar.table_id,
               tar.table_key,
               tar.connection_key,
               tar.format,
               tar.table_name,
               tar.hash_key,
               tar.deactivated_ts
            FROM TARGET tar
            WHERE tar.table_key = ?
              AND tar.format = ?
              AND tar.deactivated_ts IS NULL
        """.trimIndent()


        private const val DEACTIVATE_BY_TABLE_KEY =
            "UPDATE TARGET SET deactivated_ts = CURRENT_TIMESTAMP() WHERE table_key = ? AND deactivated_ts IS NULL"


    }
}