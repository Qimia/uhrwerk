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

    fun getByTableIdFormat(tableID: Long, format: String): List<TargetModel> =
        super.findAll(SELECT_BY_TABLE_ID_FORMAT, {
            it.setLong(1, tableID)
            it.setString(2, format)
        }, this::map)

    fun deactivateByTableId(tableId: Long): Int? =
        super.update(DEACTIVATE_BY_TABLE_ID) {
            it.setLong(1, tableId)
        }

    private fun insertParams(entity: TargetModel, insert: PreparedStatement): PreparedStatement {
        insert.setLong(1, entity.tableId!!)
        insert.setLong(2, entity.connectionId!!)
        insert.setString(3, entity.format)
        insert.setString(4, entity.tableName)
        insert.setLong(5, HashKeyUtils.targetKey(entity))
        return insert
    }

    private fun map(res: ResultSet): TargetModel {
        val target = TargetModel()
        target.id = res.getLong(1)
        target.tableId = res.getLong(2)
        target.connectionId = res.getLong(3)
        target.format = res.getString(4)
        target.tableName = res.getString(5)
        target.deactivatedTs = res.getTimestamp(6)?.toLocalDateTime()
        return target
    }


    companion object {
        private const val INSERT =
            "INSERT INTO TARGET (table_id, connection_id, format, table_name, hash_key) VALUES (?, ?, ?, ?, ?)"

        private const val SELECT_BY_ID =
            "SELECT id, table_id, connection_id, format, table_name, deactivated_ts FROM TARGET WHERE id = ?"

        private const val SELECT_BY_HASH_KEY =
            "SELECT id, table_id, connection_id, format, table_name, deactivated_ts FROM TARGET WHERE hash_key = ?"


        private const val SELECT_BY_TABLE_ID =
            "SELECT id, table_id, connection_id, format, table_name, deactivated_ts FROM TARGET WHERE table_id = ?"

        private val SELECT_BY_TABLE_ID_FORMAT = """
            SELECT tar.id,
               tar.table_id,
               tar.connection_id,
               tar.format,
               tar.table_name,
               tar.deactivated_ts
            FROM TARGET tar
                JOIN TABLE_ tab ON tab.id = tar.table_id
            WHERE tab.id = ?
              AND tar.format = ?
        """.trimIndent()


        private const val DEACTIVATE_BY_TABLE_ID =
            "UPDATE TARGET SET deactivated_ts = CURRENT_TIMESTAMP() WHERE table_id = ?"


    }
}