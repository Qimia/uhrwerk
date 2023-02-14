package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class DependencyModel(
    var id: Long? = null,
    var tableId: Long? = null,
    var tableKey: Long? = null,
    var dependencyTargetKey: Long? = null,
    var dependencyTableKey: Long? = null,
    var area: String? = null,
    var vertical: String? = null,
    var tableName: String? = null,
    var viewName: String? = null,
    var partitionMappings: Map<String, Any>? = null,
    var dependencyVariables: Array<String>? = null,
    var format: String? = null,
    var version: String? = null,
    var hashKey: Long? = null,
    var deactivatedTs: LocalDateTime? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DependencyModel) return false
        if (id != null && other.id != null) if (id != other.id) return false
        if (tableId != other.tableId) return false
        if (tableKey != other.tableKey) return false
        if (dependencyTargetKey != other.dependencyTargetKey) return false
        if (dependencyTableKey != other.dependencyTableKey) return false
        if (area != other.area) return false
        if (vertical != other.vertical) return false
        if (tableName != other.tableName) return false
        if (viewName != other.viewName) return false

        if (!partitionMappings.isNullOrEmpty()) {
            if (other.partitionMappings.isNullOrEmpty()) return false
            if (partitionMappings != other.partitionMappings) return false
        }else if (!other.partitionMappings.isNullOrEmpty()) return false

        if (!dependencyVariables.isNullOrEmpty()) {
            if (other.dependencyVariables.isNullOrEmpty()) return false
            if (!dependencyVariables.contentEquals(other.dependencyVariables)) return false
        }else if (!other.dependencyVariables.isNullOrEmpty()) return false

        if (format != other.format) return false
        if (hashKey != other.hashKey) return false
        if (version != other.version) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (tableId?.hashCode() ?: 0)
        result = 31 * result + (tableKey?.hashCode() ?: 0)
        result = 31 * result + (dependencyTargetKey?.hashCode() ?: 0)
        result = 31 * result + (dependencyTableKey?.hashCode() ?: 0)
        result = 31 * result + (area?.hashCode() ?: 0)
        result = 31 * result + (vertical?.hashCode() ?: 0)
        result = 31 * result + (tableName?.hashCode() ?: 0)
        result = 31 * result + (viewName?.hashCode() ?: 0)
        result = 31 * result + partitionMappings?.toList()?.toTypedArray().contentDeepHashCode()
        result = 31 * result + (dependencyVariables?.contentDeepHashCode() ?: 0)
        result = 31 * result + (format?.hashCode() ?: 0)
        result = 31 * result + (version?.hashCode() ?: 0)
        result = 31 * result + (hashKey?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "DependencyModel(id=$id, tableId=$tableId, tableKey=$tableKey, dependencyTargetKey=$dependencyTargetKey, dependencyTableKey=$dependencyTableKey, area=$area, vertical=$vertical, tableName=$tableName, viewName=$viewName, partitionMappings=$partitionMappings, dependencyVariables=${dependencyVariables?.contentToString()}, format=$format, version=$version, hashKey=$hashKey, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }


}