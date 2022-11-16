package io.qimia.uhrwerk.common.metastore.builders

import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.SourceModel
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.*

class TableModelBuilder : StateModelBuilder<TableModelBuilder>() {
    var id: Long? = null
    var area: String? = null
    var vertical: String? = null
    var name: String? = null
    var version: String? = null
    var className: String? = null
    var parallelism = 0
    var maxBulkSize = 0
    var partitionUnit: PartitionUnit? = null
    var partitionSize = 0
    var isPartitioned = false
    var dependencies: Array<DependencyModel>? = null
    var sources: Array<SourceModel>? = null
    var targets: Array<TargetModel>? = null
    fun id(id: Long?): TableModelBuilder {
        this.id = id
        return this
    }

    fun area(area: String): TableModelBuilder {
        this.area = area
        return this
    }

    fun vertical(vertical: String): TableModelBuilder {
        this.vertical = vertical
        return this
    }

    fun name(name: String): TableModelBuilder {
        this.name = name
        return this
    }

    fun version(version: String): TableModelBuilder {
        this.version = version
        return this
    }

    fun className(className: String?): TableModelBuilder {
        this.className = className
        return this
    }

    fun parallelism(parallelism: Int): TableModelBuilder {
        this.parallelism = parallelism
        return this
    }

    fun maxBulkSize(maxBulkSize: Int): TableModelBuilder {
        this.maxBulkSize = maxBulkSize
        return this
    }

    fun partitionUnit(partitionUnit: PartitionUnit?): TableModelBuilder {
        this.partitionUnit = partitionUnit
        return this
    }

    fun partitionSize(partitionSize: Int): TableModelBuilder {
        this.partitionSize = partitionSize
        return this
    }

    fun partitioned(partitioned: Boolean): TableModelBuilder {
        isPartitioned = partitioned
        return this
    }

    fun dependencies(dependencies: Array<DependencyModel>): TableModelBuilder {
        this.dependencies = dependencies
        return this
    }

    fun sources(sources: Array<SourceModel>): TableModelBuilder {
        this.sources = sources
        return this
    }

    fun targets(targets: Array<TargetModel>): TableModelBuilder {
        this.targets = targets
        return this
    }

    fun build(): TableModel {
        return TableModel(
            id,
            area,
            vertical,
            name,
            version,
            className,
            parallelism,
            maxBulkSize,
            partitionUnit,
            partitionSize,
            isPartitioned,
            dependencies,
            sources,
            targets,
        )
    }

    override fun getThis() = this

}