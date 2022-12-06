package io.qimia.uhrwerk.common.metastore.model

import io.qimia.uhrwerk.common.model.TargetModel
import java.sql.Timestamp
import java.time.LocalDateTime

data class TableModel(
    var id: Long? = null,
    var area: String? = null,
    var vertical: String? = null,
    var name: String? = null,
    var version: String? = null,
    var className: String? = null,
    var parallelism: Int? = null,
    var maxBulkSize: Int? = null,
    var partitionUnit: PartitionUnit? = null,
    var partitionSize: Int? = null,
    var partitioned: Boolean = false,
    var dependencies: Array<DependencyModel>? = null,
    var sources: Array<SourceModel2>? = null,
    var targets: Array<TargetModel>? = null,
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
        if (other !is TableModel) return false
        if (id != null && other.id != null) if (id != other.id) return false
        if (area != other.area) return false
        if (vertical != other.vertical) return false
        if (name != other.name) return false
        if (version != other.version) return false
        if (className != other.className) return false
        if (parallelism != other.parallelism) return false
        if (maxBulkSize != other.maxBulkSize) return false
        if (partitionUnit != other.partitionUnit) return false
        if (partitionSize != other.partitionSize) return false
        if (partitioned != other.partitioned) return false
        if (dependencies != null) {
            if (other.dependencies == null) return false
            if (!dependencies.contentEquals(other.dependencies)) return false
        } else if (other.dependencies != null) return false
        if (sources != null) {
            if (other.sources == null) return false
            if (!sources.contentEquals(other.sources)) return false
        } else if (other.sources != null) return false
        if (targets != null) {
            if (other.targets == null) return false
            if (!targets.contentEquals(other.targets)) return false
        } else if (other.targets != null) return false
        if (description != other.description) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (area?.hashCode() ?: 0)
        result = 31 * result + (vertical?.hashCode() ?: 0)
        result = 31 * result + (name?.hashCode() ?: 0)
        result = 31 * result + (version?.hashCode() ?: 0)
        result = 31 * result + (className?.hashCode() ?: 0)
        result = 31 * result + (parallelism ?: 0)
        result = 31 * result + (maxBulkSize ?: 0)
        result = 31 * result + (partitionUnit?.hashCode() ?: 0)
        result = 31 * result + (partitionSize ?: 0)
        result = 31 * result + partitioned.hashCode()
        result = 31 * result + (dependencies?.contentHashCode() ?: 0)
        result = 31 * result + (sources?.contentHashCode() ?: 0)
        result = 31 * result + (targets?.contentHashCode() ?: 0)
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "TableModel(id=$id, area=$area, vertical=$vertical, name=$name, version=$version, className=$className, parallelism=$parallelism, maxBulkSize=$maxBulkSize, partitionUnit=$partitionUnit, partitionSize=$partitionSize, partitioned=$partitioned, dependencies=${dependencies?.contentToString()}, sources=${sources?.contentToString()}, targets=${targets?.contentToString()}, description=$description, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }
}