package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class Partition(
    var id: Long? = null,
    var tableKey: Long? = null,
    var targetKey: Long? = null,
    var partitionTs: LocalDateTime? = null,
    var partitionUnit: PartitionUnit? = null,
    var partitionSize: Int = 0,
    var partitioned: Boolean = false,
    var bookmarked: Boolean = false,
    var maxBookmark: String? = null,
    var partitionValues: Map<String, Any>? = null,
    var partitionPath: String? = null,
    var deactivatedTs: LocalDateTime? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Partition) return false
        if (tableKey != other.tableKey) return false
        if (targetKey != other.targetKey) return false
        if (partitionTs != other.partitionTs) return false
        if (partitionUnit != other.partitionUnit) return false
        if (partitionSize != other.partitionSize) return false
        if (partitioned != other.partitioned) return false
        if (bookmarked != other.bookmarked) return false
        if (maxBookmark != other.maxBookmark) return false
        if (!partitionValues.isNullOrEmpty()) {
            if (other.partitionValues.isNullOrEmpty()) return false
            if (partitionValues!! != other.partitionValues) return false
        } else if (!other.partitionValues.isNullOrEmpty()) return false
        if (deactivatedTs != other.deactivatedTs) return false
        return true
    }

    override fun hashCode(): Int {
        var result = tableKey?.hashCode() ?: 0
        result = 31 * result + (targetKey?.hashCode() ?: 0)
        result = 31 * result + (partitionTs?.hashCode() ?: 0)
        result = 31 * result + (partitionUnit?.hashCode() ?: 0)
        result = 31 * result + partitionSize
        result = 31 * result + partitioned.hashCode()
        result = 31 * result + bookmarked.hashCode()
        result = 31 * result + (maxBookmark?.hashCode() ?: 0)
        result = 31 * result + (partitionValues?.hashCode() ?: 0)
        result = 31 * result + (partitionPath?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "Partition(id=$id, tableKey=$tableKey, targetKey=$targetKey, partitionTs=$partitionTs, partitionUnit=$partitionUnit, partitionSize=$partitionSize, partitioned=$partitioned, bookmarked=$bookmarked, maxBookmark=$maxBookmark, partitionValues=$partitionValues, partitionPath=$partitionPath, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }


}