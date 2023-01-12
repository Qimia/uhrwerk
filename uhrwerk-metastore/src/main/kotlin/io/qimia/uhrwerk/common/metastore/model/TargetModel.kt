package io.qimia.uhrwerk.common.model

import io.qimia.uhrwerk.common.metastore.model.BaseModel
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.TableModel
import java.sql.Timestamp
import java.time.LocalDateTime

data class TargetModel(
    var id: Long? = null,
    var format: String? = null,
    var tableId: Long? = null,
    var connectionId: Long? = null,
    var connection: ConnectionModel? = null,
    var table: TableModel? = null,
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
        if (format != other.format) return false
        if (tableId != other.tableId) return false
        if (connectionId != other.connectionId) return false
        if (connection?.id != other.connection?.id) return false
        if (table?.id != other.table?.id) return false
        if (description != other.description) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (format?.hashCode() ?: 0)
        result = 31 * result + (tableId?.hashCode() ?: 0)
        result = 31 * result + (connectionId?.hashCode() ?: 0)
        result = 31 * result + (connection?.hashCode() ?: 0)
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "TargetModel(id=$id, format=$format, tableId=$tableId, connectionId=$connectionId, description=$description, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }
}