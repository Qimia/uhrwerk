package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class FunctionCallModel(
    var id: Long? = null,
    var tableId: Long? = null,
    var tableKey: Long? = null,
    var functionKey: Long? = null,
    var functionName: String? = null,
    var functionDefinition: FunctionDefinitionModel? = null,
    var functionCallOrder: Int? = null,
    var args: Map<String, Any>? = null,
    var inputViews: Map<String, String>? = null,
    var output: String? = null,
    var deactivatedTs: LocalDateTime? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id;
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionCallModel) return false

        if (id != null && other.id != null) if (id != other.id) return false
        if (functionKey != other.functionKey) return false
        if (tableId != other.tableId) return false
        if (tableKey != other.tableKey) return false
        if (functionName != other.functionName) return false
        if (functionCallOrder != other.functionCallOrder) return false
        if (args != other.args) return false
        if (inputViews != null) {
            if (other.inputViews == null) return false
            if (inputViews != other.inputViews) return false
        } else if (other.inputViews != null) return false
        if (output != other.output) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        return updatedTs == other.updatedTs
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (tableId?.hashCode() ?: 0)
        result = 31 * result + (tableKey?.hashCode() ?: 0)
        result = 31 * result + (functionKey?.hashCode() ?: 0)
        result = 31 * result + (functionName?.hashCode() ?: 0)
        result = 31 * result + (functionCallOrder ?: 0)
        result = 31 * result + (args?.hashCode() ?: 0)
        result = 31 * result + inputViews?.toList()?.toTypedArray().contentDeepHashCode()
        result = 31 * result + (output?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "FunctionCallModel(id=$id, tableId=$tableId, tableKey=$tableKey, functionKey=$functionKey, functionName=$functionName, order=$functionCallOrder, args=$args, inputViews=$inputViews, output=$output, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }

}
