package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.FunctionCallResult
import io.qimia.uhrwerk.common.metastore.model.FunctionCallModel
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.repo.FunctionCallRepo
import io.qimia.uhrwerk.repo.FunctionDefinitionRepo
import java.sql.SQLException

class FunctionCallDAO {
    private val funCallRepo = FunctionCallRepo()
    private val funDefRepo = FunctionDefinitionRepo()

    fun getById(id: Long): FunctionCallModel? {
        val funcCall = funCallRepo.getById(id)
        if (funcCall != null) {
            funcCall.functionDefinition = funDefRepo.getByHashKey(funcCall.functionKey!!)
        }
        return funcCall
    }

    fun getByTableId(tableId: Long): List<FunctionCallModel> {
        val functions = funCallRepo.getByTableId(tableId)
        functions.forEach { it.functionDefinition = funDefRepo.getByHashKey(it.functionKey!!) }
        return functions
    }

    fun deactivateByTableKey(tableKey: Long): Int? {
        return funCallRepo.deactivateByTableKey(tableKey)
    }

    fun save(functions: List<FunctionCallModel>) = functions.map { save(it) }

    fun save(function: FunctionCallModel): FunctionCallResult {
        val result = FunctionCallResult()
        function.functionKey = HashKeyUtils.functionKey(function.functionName)
        val functionDefinition = funDefRepo.getByHashKey(function.functionKey!!)
        try {
            if (functionDefinition == null) {
                result.message = String.format(
                    "A FunctionDefinition with name=%s doesn't exists in the Metastore.",
                    function.functionName
                )
                return result
            }
            function.functionDefinition = functionDefinition
            result.functionCall = funCallRepo.save(function)
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