package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.FunctionCallModel
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import io.qimia.uhrwerk.repo.RepoUtils.jsonToMap
import io.qimia.uhrwerk.repo.RepoUtils.jsonToStringMap
import io.qimia.uhrwerk.repo.RepoUtils.toJson


class FunctionCallRepo : BaseRepo<FunctionCallModel>() {
    fun save(functionCall: FunctionCallModel): FunctionCallModel? =
        super.insert(functionCall, INSERT) {
            insertParams(functionCall, it)
        }

    fun save(secrets: List<FunctionCallModel>): List<FunctionCallModel> =
        secrets.map { save(it)!! }

    fun getById(id: Long): FunctionCallModel? =
        super.find(
            SELECT_BY_ID, {
                it.setLong(1, id)
            }, this::map
        )

    fun getByTableId(tableId: Long): List<FunctionCallModel> =
        super.findAll(SELECT_BY_TABLE_ID, {
            it.setLong(1, tableId)
        }, this::map)

    fun deactivateById(id: Long): Int? =
        super.update(
            DEACTIVATE_BY_ID,
        ) {
            it.setLong(1, id)
        }

    fun deactivateByTableKey(tableKey: Long): Int? =
        super.update(DEACTIVATE_BY_TABLE_KEY) {
            it.setLong(1, tableKey)
        }

    private fun insertParams(
        entity: FunctionCallModel,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setLong(1, entity.tableId!!)
        insert.setLong(2, entity.tableKey!!)

        if (entity.functionKey == null)
            entity.functionKey = HashKeyUtils.functionKey(entity.functionName)

        insert.setLong(3, entity.functionKey!!)

        insert.setString(4, entity.functionName)
        insert.setInt(5, entity.functionCallOrder!!)

        if (entity.args.isNullOrEmpty())
            insert.setNull(6, Types.VARCHAR)
        else
            insert.setString(6, toJson(entity.args!!))

        if (entity.inputViews.isNullOrEmpty())
            insert.setNull(7, Types.VARCHAR)
        else
            insert.setString(7, toJson(entity.inputViews!!))

        insert.setString(8, entity.output)
        return insert
    }

    private fun map(res: ResultSet): FunctionCallModel {
        val functionCall = FunctionCallModel()
        functionCall.id = res.getLong("id")
        functionCall.tableId = res.getLong("table_id")
        functionCall.tableKey = res.getLong("table_key")
        functionCall.functionKey = res.getLong("function_key")
        functionCall.functionName = res.getString("function_name")
        functionCall.functionCallOrder = res.getInt("function_call_order")

        val args = res.getString("args")
        if (!args.isNullOrEmpty())
            functionCall.args = jsonToMap(args)

        val inputViews = res.getString("input_views")
        if (!inputViews.isNullOrEmpty())
            functionCall.inputViews = jsonToStringMap(inputViews)

        functionCall.output = res.getString("output")

        functionCall.deactivatedTs = res.getTimestamp("deactivated_ts")?.toLocalDateTime()

        return functionCall
    }


    companion object {
        private val COLUMNS = listOf(
            "id",
            "table_id",
            "table_key",
            "function_key",
            "function_name",
            "function_call_order",
            "args",
            "input_views",
            "output",
            "deactivated_ts"
        )

        private val COLUMNS_STR = columnsToString(COLUMNS)

        private val INSERT = insertSql("FUNCTION_CALL", COLUMNS)


        private val SELECT_BY_ID = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM FUNCTION_CALL \n" +
                "WHERE id = ?"

        private val SELECT_BY_TABLE_ID = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM FUNCTION_CALL \n" +
                "WHERE table_id = ?"

        private const val DEACTIVATE_BY_ID =
            "UPDATE FUNCTION_CALL SET deactivated_ts = CURRENT_TIMESTAMP() WHERE id = ?"

        private const val DEACTIVATE_BY_TABLE_KEY =
            "UPDATE FUNCTION_CALL SET deactivated_ts = CURRENT_TIMESTAMP() WHERE table_key = ? AND deactivated_ts IS NULL"

    }
}