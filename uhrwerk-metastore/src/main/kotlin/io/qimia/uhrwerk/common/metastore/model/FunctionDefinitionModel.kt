package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class FunctionDefinitionModel(
    var id: Long? = null,
    var name: String? = null,
    var type: FunctionType? = null,
    var className: String? = null,
    var sqlQuery: String? = null,
    var params: Array<String>? = null,
    var inputViews: Array<String>? = null,
    var output: String? = null,
    var description: String? = null,
    var hashKey: Long? = null,
    var deactivatedTs: LocalDateTime? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id;
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionDefinitionModel) return false

        if (id != null && other.id != null) if (id != other.id) return false
        if (name != other.name) return false
        if (type != other.type) return false
        if (className != other.className) return false
        if (sqlQuery != other.sqlQuery) return false
        if (params != null) {
            if (other.params == null) return false
            if (!params.contentEquals(other.params)) return false
        } else if (other.params != null) return false
        if (inputViews != null) {
            if (other.inputViews == null) return false
            if (!inputViews.contentEquals(other.inputViews)) return false
        } else if (other.inputViews != null) return false
        if (output != other.output) return false
        if (description != other.description) return false
        if (hashKey != null && other.hashKey != null) if (hashKey != other.hashKey) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        return updatedTs == other.updatedTs
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (name?.hashCode() ?: 0)
        result = 31 * result + (type?.hashCode() ?: 0)
        result = 31 * result + (className?.hashCode() ?: 0)
        result = 31 * result + (sqlQuery?.hashCode() ?: 0)
        result = 31 * result + (params?.contentHashCode() ?: 0)
        result = 31 * result + (inputViews?.contentHashCode() ?: 0)
        result = 31 * result + (output?.hashCode() ?: 0)
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (hashKey?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

}
