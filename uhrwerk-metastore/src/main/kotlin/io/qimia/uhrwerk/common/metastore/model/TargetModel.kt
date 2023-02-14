package io.qimia.uhrwerk.common.model

import io.qimia.uhrwerk.common.metastore.model.BaseModel
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import java.sql.Timestamp
import java.time.LocalDateTime

data class TargetModel(
    var id: Long? = null,
    var tableId: Long? = null,
    var tableKey: Long? = null,
    var connectionKey: Long? = null,
    var format: String? = null,
    var tableName: String? = null,
    var hashKey: Long? = null,
    var connection: ConnectionModel? = null,
    var description: String? = null,
    var deactivatedTs: LocalDateTime? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TargetModel) return false
        if (id != null && other.id != null) if (id != other.id) return false
        if (tableId != other.tableId) return false
        if (tableKey != other.tableKey) return false
        if (connectionKey != other.connectionKey) return false
        if (format != other.format) return false
        if (tableName != other.tableName) return false
        if (hashKey != other.hashKey) return false
        if (description != other.description) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (tableId?.hashCode() ?: 0)
        result = 31 * result + (tableKey?.hashCode() ?: 0)
        result = 31 * result + (connectionKey?.hashCode() ?: 0)
        result = 31 * result + (format?.hashCode() ?: 0)
        result = 31 * result + (tableName?.hashCode() ?: 0)
        result = 31 * result + (hashKey?.hashCode() ?: 0)
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "TargetModel(id=$id, tableId=$tableId, tableKey=$tableKey, connectionKey=$connectionKey, format=$format, tableName=$tableName, hashKey=$hashKey, description=$description, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }

}