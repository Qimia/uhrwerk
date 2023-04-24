package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.FunctionDefinitionModel
import java.io.Serializable

class FunctionDefinitionResult : Serializable {
    var newFunctionDefinition: FunctionDefinitionModel? = null
    var oldFunctionDefinition: FunctionDefinitionModel? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionDefinitionResult) return false

        if (newFunctionDefinition != other.newFunctionDefinition) return false
        if (oldFunctionDefinition != other.oldFunctionDefinition) return false
        if (isSuccess != other.isSuccess) return false
        if (isError != other.isError) return false
        if (message != other.message) return false
        return exception == other.exception
    }

    override fun hashCode(): Int {
        var result = newFunctionDefinition?.hashCode() ?: 0
        result = 31 * result + (oldFunctionDefinition?.hashCode() ?: 0)
        result = 31 * result + isSuccess.hashCode()
        result = 31 * result + isError.hashCode()
        result = 31 * result + (message?.hashCode() ?: 0)
        result = 31 * result + (exception?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "FunctionDefinitionResult(newFunctionDefinition=$newFunctionDefinition, oldFunctionDefinition=$oldFunctionDefinition, isSuccess=$isSuccess, isError=$isError, message=$message, exception=$exception)"
    }

}