package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class Partition(
    var id: Long? = null,
    var targetId: Long? = null,
    var partitionTs: LocalDateTime? = null,
    var partitionUnit: PartitionUnit? = null,
    var partitionSize: Int = 0,
    var partitioned: Boolean = false,
    var bookmarked: Boolean = false,
    var maxBookmark: String? = null,
    var partitionValues: Map<String, Any>? = null,
    var partitionPath: String? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Partition) return false
        if (id != null && other.id != null) if (id != other.id) return false
        if (targetId != other.targetId) return false
        if (partitionTs != other.partitionTs) return false
        if (partitionUnit != other.partitionUnit) return false
        if (partitionSize != other.partitionSize) return false
        if (partitioned != other.partitioned) return false
        if (bookmarked != other.bookmarked) return false
        if (maxBookmark != other.maxBookmark) return false
        if (!partitionValues.isNullOrEmpty()) {
            if (other.partitionValues.isNullOrEmpty()) return false
            if (partitionValues!! != other.partitionValues) return false
        }
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (targetId?.hashCode() ?: 0)
        result = 31 * result + (partitionTs?.hashCode() ?: 0)
        result = 31 * result + (partitionUnit?.hashCode() ?: 0)
        result = 31 * result + (partitionSize ?: 0)
        result = 31 * result + (partitioned?.hashCode() ?: 0)
        result = 31 * result + (bookmarked?.hashCode() ?: 0)
        result = 31 * result + (maxBookmark?.hashCode() ?: 0)
        result =
            31 * result + (partitionValues?.toList()?.toTypedArray().contentDeepHashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }
}