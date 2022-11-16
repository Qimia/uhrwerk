package io.qimia.uhrwerk.common.metastore.builders

import io.qimia.uhrwerk.common.metastore.model.PartitionDependency

class PartitionDependencyBuilder {
    var id: Long? = null
    var partitionId: Long? = null
    var dependencyPartitionId: Long? = null
    fun id(id: Long?): PartitionDependencyBuilder {
        this.id = id
        return this
    }

    fun partitionId(partitionId: Long?): PartitionDependencyBuilder {
        this.partitionId = partitionId
        return this
    }

    fun dependencyPartitionId(dependencyPartitionId: Long?): PartitionDependencyBuilder {
        this.dependencyPartitionId = dependencyPartitionId
        return this
    }

    fun build(): PartitionDependency {
        return PartitionDependency(id, partitionId, dependencyPartitionId)
    }
}