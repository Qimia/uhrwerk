package io.qimia.uhrwerk.common.metastore.model

import io.qimia.uhrwerk.common.model.TargetModel
import java.sql.Timestamp
import java.time.LocalDateTime

data class DependencyModel(
    var id: Long? = null,
    var tableId: Long? = null,
    var dependencyTargetId: Long? = null,
    var dependencyTableId: Long? = null,
    var table: TableModel? = null,
    var dependencyTable: TableModel? = null,
    var dependencyTarget: TargetModel? = null,
    var area: String? = null,
    var vertical: String? = null,
    var tableName: String? = null,
    var viewName: String? = null,
    var partitionMappings: Map<String, Any>? = null,
    var dependencyVariables: Array<String>? = null,
    var format: String? = null,
    var version: String? = null,
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
        if (dependencyTargetId != other.dependencyTargetId) return false
        if (dependencyTableId != other.dependencyTableId) return false
        if (area != other.area) return false
        if (vertical != other.vertical) return false
        if (tableName != other.tableName) return false
        if (viewName != other.viewName) return false

        if (!partitionMappings.isNullOrEmpty()) {
            if (other.partitionMappings.isNullOrEmpty()) return false
            if (partitionMappings != other.partitionMappings) return false
        }

        if (!dependencyVariables.isNullOrEmpty()) {
            if (other.dependencyVariables.isNullOrEmpty()) return false
            if (!dependencyVariables.contentEquals(other.dependencyVariables)) return false
        }

        if (format != other.format) return false
        if (version != other.version) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (tableId?.hashCode() ?: 0)
        result = 31 * result + (dependencyTargetId?.hashCode() ?: 0)
        result = 31 * result + (dependencyTableId?.hashCode() ?: 0)
        result = 31 * result + (area?.hashCode() ?: 0)
        result = 31 * result + (vertical?.hashCode() ?: 0)
        result = 31 * result + (tableName?.hashCode() ?: 0)
        result = 31 * result + (viewName?.hashCode() ?: 0)
        result = 31 * result + (partitionMappings?.hashCode() ?: 0)
        result = 31 * result + (dependencyVariables?.contentDeepHashCode() ?: 0)
        result = 31 * result + (format?.hashCode() ?: 0)
        result = 31 * result + (version?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "DependencyModel(id=$id, tableId=$tableId, dependencyTargetId=$dependencyTargetId, dependencyTableId=$dependencyTableId, area=$area, vertical=$vertical, tableName=$tableName, viewName=$viewName, partitionMappings=$partitionMappings, dependencyVariables=${dependencyVariables?.contentToString()}, format=$format, version=$version, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }


}