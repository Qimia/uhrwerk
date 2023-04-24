package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.FunctionDefinitionModel
import io.qimia.uhrwerk.common.metastore.model.FunctionType
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.repo.RepoUtils.jsonToArray
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import io.qimia.uhrwerk.repo.RepoUtils.toJson


class FunctionDefinitionRepo : BaseRepo<FunctionDefinitionModel>() {
    fun save(secret: FunctionDefinitionModel): FunctionDefinitionModel? =
        super.insert(secret, INSERT) {
            insertParams(secret, it)
        }
    fun getById(id: Long): FunctionDefinitionModel? =
        super.find(
            SELECT_BY_ID, {
                it.setLong(1, id)
            }, this::map
        )

    fun getByHashKey(hashKey: Long): FunctionDefinitionModel? =
        super.getByHashKey(
            SELECT_BY_HASH_KEY, {
                it.setLong(1, hashKey)
            }, this::map
        )

    fun deactivateById(id: Long): Int? =
        super.update(
            DEACTIVATE_BY_ID,
        ) {
            it.setLong(1, id)
        }


    private fun insertParams(
        entity: FunctionDefinitionModel,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setString(1, entity.name)
        insert.setString(2, entity.type!!.name)
        insert.setString(3, entity.className)
        insert.setString(4, entity.sqlQuery)

        if (entity.params.isNullOrEmpty())
            insert.setNull(5, Types.VARCHAR)
        else
            insert.setString(5, toJson(entity.params!!))

        if (entity.inputViews.isNullOrEmpty())
            insert.setNull(6, Types.VARCHAR)
        else
            insert.setString(6, toJson(entity.inputViews!!))


        insert.setString(7, entity.output)

        entity.hashKey = HashKeyUtils.functionKey(entity.name!!)
        insert.setLong(8, entity.hashKey!!)

        return insert
    }

    private fun map(res: ResultSet): FunctionDefinitionModel {
        val functionDefinition = FunctionDefinitionModel()
        functionDefinition.id = res.getLong("id")
        functionDefinition.name = res.getString("name")

        val functionType = res.getString("type")
        if (!functionType.isNullOrEmpty())
            functionDefinition.type = FunctionType.valueOf(functionType.uppercase())

        functionDefinition.className = res.getString("class_name")
        functionDefinition.sqlQuery = res.getString("sql_query")

        val params = res.getString("params")
        if (!params.isNullOrEmpty())
            functionDefinition.params = jsonToArray(params)

        val inputViews = res.getString("input_views")
        if (!inputViews.isNullOrEmpty())
            functionDefinition.inputViews = jsonToArray(inputViews)

        functionDefinition.output = res.getString("output")

        functionDefinition.hashKey = res.getLong("hash_key")

        functionDefinition.deactivatedTs = res.getTimestamp("deactivated_ts")?.toLocalDateTime()

        return functionDefinition
    }


    companion object {
        private val COLUMNS = listOf(
            "id",
            "name",
            "type",
            "class_name",
            "sql_query",
            "params",
            "input_views",
            "output",
            "hash_key",
            "deactivated_ts"
        )

        private val COLUMNS_STR = columnsToString(COLUMNS)

        private val INSERT = insertSql("FUNCTION_DEFINITION", COLUMNS)


        private val SELECT_BY_ID = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM FUNCTION_DEFINITION \n" +
                "WHERE id = ?"

        private val SELECT_BY_HASH_KEY = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM FUNCTION_DEFINITION \n" +
                "WHERE hash_key = ? AND deactivated_ts IS NULL"

        private const val DEACTIVATE_BY_ID =
            "UPDATE FUNCTION_DEFINITION SET deactivated_ts = CURRENT_TIMESTAMP() WHERE id = ?"


    }
}