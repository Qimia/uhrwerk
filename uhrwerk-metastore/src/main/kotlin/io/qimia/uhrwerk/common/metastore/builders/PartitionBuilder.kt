package io.qimia.uhrwerk.common.metastore.builders

import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import java.time.LocalDateTime

class PartitionBuilder {
    var id: Long? = null
    var targetId: Long? = null
    var partitionTs: LocalDateTime? = null
    var partitionUnit: PartitionUnit? = null
    var partitionSize = 0
    var isPartitioned = false
    var isBookmarked = false
    var maxBookmark: String? = null
    fun id(id: Long?): PartitionBuilder {
        this.id = id
        return this
    }

    fun targetId(targetId: Long?): PartitionBuilder {
        this.targetId = targetId
        return this
    }

    fun partitionTs(partitionTs: LocalDateTime?): PartitionBuilder {
        this.partitionTs = partitionTs
        return this
    }

    fun partitionUnit(partitionUnit: PartitionUnit?): PartitionBuilder {
        this.partitionUnit = partitionUnit
        return this
    }

    fun partitionSize(partitionSize: Int): PartitionBuilder {
        this.partitionSize = partitionSize
        return this
    }

    fun partitioned(partitioned: Boolean): PartitionBuilder {
        isPartitioned = partitioned
        return this
    }

    fun bookmarked(bookmarked: Boolean): PartitionBuilder {
        isBookmarked = bookmarked
        return this
    }

    fun maxBookmark(maxBookmark: String?): PartitionBuilder {
        this.maxBookmark = maxBookmark
        return this
    }

    fun build(): Partition {
        return Partition(
            id,
            targetId,
            partitionTs,
            partitionUnit,
            partitionSize,
            isPartitioned,
            isBookmarked,
            maxBookmark
        )
    }
}