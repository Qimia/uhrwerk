package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class SourceModel(
    var id: Long? = null,
    var tableId: Long? = null,
    var connectionId: Long? = null,
    var path: String? = null,
    var format: String? = null,
    var connection: ConnectionModel? = null,
    var table: TableModel? = null,
    var partitionUnit: PartitionUnit? = null,
    var partitionSize: Int? = null,
    var parallelLoadQuery: String? = null,
    var parallelLoadColumn: String? = null,
    var parallelLoadNum: Int? = null,
    var selectQuery: String? = null,
    var selectColumn: String? = null,
    var partitioned: Boolean = false,
    var autoLoad: Boolean = true,
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
        if (other !is SourceModel) return false

        if (id != null && other.id != null) if (id != other.id) return false
        if (tableId != other.tableId) return false
        if (connectionId != other.connectionId) return false
        if (path != other.path) return false
        if (format != other.format) return false
        if (connection != other.connection) return false
        if (table != other.table) return false
        if (partitionUnit != other.partitionUnit) return false
        if (partitionSize != other.partitionSize) return false
        if (parallelLoadQuery != other.parallelLoadQuery) return false
        if (parallelLoadColumn != other.parallelLoadColumn) return false
        if (parallelLoadNum != other.parallelLoadNum) return false
        if (selectQuery != other.selectQuery) return false
        if (selectColumn != other.selectColumn) return false
        if (partitioned != other.partitioned) return false
        if (autoLoad != other.autoLoad) return false
        if (description != other.description) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (tableId?.hashCode() ?: 0)
        result = 31 * result + (connectionId?.hashCode() ?: 0)
        result = 31 * result + (path?.hashCode() ?: 0)
        result = 31 * result + (format?.hashCode() ?: 0)
        result = 31 * result + (connection?.hashCode() ?: 0)
        result = 31 * result + (table?.hashCode() ?: 0)
        result = 31 * result + (partitionUnit?.hashCode() ?: 0)
        result = 31 * result + (partitionSize ?: 0)
        result = 31 * result + (parallelLoadQuery?.hashCode() ?: 0)
        result = 31 * result + (parallelLoadColumn?.hashCode() ?: 0)
        result = 31 * result + (parallelLoadNum ?: 0)
        result = 31 * result + (selectQuery?.hashCode() ?: 0)
        result = 31 * result + (selectColumn?.hashCode() ?: 0)
        result = 31 * result + partitioned.hashCode()
        result = 31 * result + autoLoad.hashCode()
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "SourceModel(id=$id, tableId=$tableId, connectionId=$connectionId, path=$path, format=$format, partitionUnit=$partitionUnit, partitionSize=$partitionSize, parallelLoadQuery=$parallelLoadQuery, parallelLoadColumn=$parallelLoadColumn, parallelLoadNum=$parallelLoadNum, selectQuery=$selectQuery, selectColumn=$selectColumn, partitioned=$partitioned, autoLoad=$autoLoad, description=$description, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }
}