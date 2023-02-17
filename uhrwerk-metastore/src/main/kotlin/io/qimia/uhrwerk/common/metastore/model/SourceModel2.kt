package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class SourceModel2(
    var id: Long? = null,
    var tableId: Long? = null,
    var tableKey: Long? = null,
    var connectionKey: Long? = null,
    var path: String? = null,
    var format: String? = null,
    var connection: ConnectionModel? = null,
    var ingestionMode: IngestionMode = IngestionMode.ALL,
    var intervalTempUnit: PartitionUnit? = null,
    var intervalTempSize: Int = 0,
    var intervalColumn: String? = null,
    var deltaColumn: String? = null,
    var selectQuery: String? = null,
    var sourceVariables: Array<String>? = null,
    var parallelLoad: Boolean = false,
    var parallelPartitionQuery: String? = null,
    var parallelPartitionColumn: String? = null,
    var parallelPartitionNum: Int? = 0,
    var autoLoad: Boolean = true,
    var viewName: String? = null,
    var hashKey: Long? = null,
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
        if (other !is SourceModel2) return false

        if (id != null && other.id != null) if (id != other.id) return false
        if (tableId != other.tableId) return false
        if (tableKey != other.tableKey) return false
        if (connectionKey != other.connectionKey) return false
        if (path != other.path) return false
        if (format != other.format) return false
        if (ingestionMode != other.ingestionMode) return false
        if (intervalTempUnit != other.intervalTempUnit) return false
        if (intervalTempSize != other.intervalTempSize) return false
        if (intervalColumn != other.intervalColumn) return false
        if (deltaColumn != other.deltaColumn) return false
        if (selectQuery != other.selectQuery) return false

        if (!sourceVariables.isNullOrEmpty()) {
            if (other.sourceVariables.isNullOrEmpty()) return false
            if (!sourceVariables.contentEquals(other.sourceVariables)) return false
        } else if (!other.sourceVariables.isNullOrEmpty()) return false

        if (parallelLoad != other.parallelLoad) return false
        if (parallelPartitionQuery != other.parallelPartitionQuery) return false
        if (parallelPartitionColumn != other.parallelPartitionColumn) return false
        if (parallelPartitionNum != other.parallelPartitionNum) return false
        if (autoLoad != other.autoLoad) return false
        if (hashKey != null && other.hashKey != null) if (hashKey != other.hashKey) return false
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
        result = 31 * result + (path?.hashCode() ?: 0)
        result = 31 * result + (format?.hashCode() ?: 0)
        result = 31 * result + (ingestionMode?.hashCode() ?: 0)
        result = 31 * result + (intervalTempUnit?.hashCode() ?: 0)
        result = 31 * result + (intervalTempSize ?: 0)
        result = 31 * result + (intervalColumn?.hashCode() ?: 0)
        result = 31 * result + (deltaColumn?.hashCode() ?: 0)
        result = 31 * result + (selectQuery?.hashCode() ?: 0)
        result = 31 * result + (sourceVariables?.contentHashCode() ?: 0)
        result = 31 * result + parallelLoad.hashCode()
        result = 31 * result + (parallelPartitionQuery?.hashCode() ?: 0)
        result = 31 * result + (parallelPartitionColumn?.hashCode() ?: 0)
        result = 31 * result + (parallelPartitionNum ?: 0)
        result = 31 * result + autoLoad.hashCode()
        result = 31 * result + (hashKey?.hashCode() ?: 0)
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "SourceModel2(id=$id, tableId=$tableId, tableKey=$tableKey, connectionKey=$connectionKey, path=$path, format=$format, ingestionMode=$ingestionMode, intervalTempUnit=$intervalTempUnit, intervalTempSize=$intervalTempSize, intervalColumn=$intervalColumn, deltaColumn=$deltaColumn, selectQuery=$selectQuery, sourceVariables=${sourceVariables?.contentToString()}, parallelLoad=$parallelLoad, parallelPartitionQuery=$parallelPartitionQuery, parallelPartitionColumn=$parallelPartitionColumn, parallelPartitionNum=$parallelPartitionNum, autoLoad=$autoLoad, hashKey=$hashKey, description=$description, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }


}