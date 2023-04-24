package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.FunctionDefinitionResult
import io.qimia.uhrwerk.common.metastore.model.FunctionDefinitionModel
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.repo.FunctionDefinitionRepo
import java.sql.SQLException

class FunctionDefinitionDAO{
    private val repo = FunctionDefinitionRepo()

    fun getByHashKey(hashKey: Long): FunctionDefinitionModel? =
        repo.getByHashKey(hashKey)

    fun save(function: FunctionDefinitionModel, overwrite: Boolean): FunctionDefinitionResult {
        val result = FunctionDefinitionResult()
        result.oldFunctionDefinition = getByHashKey(HashKeyUtils.functionKey(function.name))
        try {
            if (!overwrite && result.oldFunctionDefinition != null) {
                result.message = String.format(
                    "A FunctionDefinition with name=%s already exists in the Metastore.",
                    function.name
                )
                return result
            }
            if (result.oldFunctionDefinition != null)
                repo.deactivateById(result.oldFunctionDefinition!!.id!!)

            result.newFunctionDefinition = repo.save(function)
            result.isSuccess = true

        } catch (e: SQLException) {
            result.isError = true
            result.isSuccess = false
            result.exception = e
            result.message = e.message
        }
        return result
    }

}