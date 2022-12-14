package io.qimia.uhrwerk.common.metastore.builders

import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.*

class DependencyModelBuilder : StateModelBuilder<DependencyModelBuilder>() {
    var id: Long? = null
    var tableId: Long? = null
    var dependencyTargetId: Long? = null
    var dependencyTableId: Long? = null
    var table: TableModel? = null
    var dependencyTable: TableModel? = null
    var dependencyTarget: TargetModel? = null
    var area: String? = null
    var vertical: String? = null
    var tableName: String? = null
    var viewName: String? = null
    var format: String? = null
    var version: String? = null
    fun id(id: Long?): DependencyModelBuilder {
        this.id = id
        return this
    }

    fun tableId(tableId: Long): DependencyModelBuilder {
        this.tableId = tableId
        return this
    }

    fun dependencyTargetId(dependencyTargetId: Long): DependencyModelBuilder {
        this.dependencyTargetId = dependencyTargetId
        return this
    }

    fun dependencyTableId(dependencyTableId: Long): DependencyModelBuilder {
        this.dependencyTableId = dependencyTableId
        return this
    }

    fun table(table: TableModel?): DependencyModelBuilder {
        this.table = table
        return this
    }

    fun dependencyTable(dependencyTable: TableModel?): DependencyModelBuilder {
        this.dependencyTable = dependencyTable
        return this
    }

    fun dependencyTarget(dependencyTarget: TargetModel?): DependencyModelBuilder {
        this.dependencyTarget = dependencyTarget
        return this
    }

    fun area(area: String?): DependencyModelBuilder {
        this.area = area
        return this
    }

    fun vertical(vertical: String?): DependencyModelBuilder {
        this.vertical = vertical
        return this
    }

    fun tableName(tableName: String?): DependencyModelBuilder {
        this.tableName = tableName
        return this
    }

    fun viewName(viewName: String?): DependencyModelBuilder {
        this.viewName = viewName
        return this
    }

    fun format(format: String?): DependencyModelBuilder {
        this.format = format
        return this
    }

    fun version(version: String?): DependencyModelBuilder {
        this.version = version
        return this
    }


    fun build(): DependencyModel {
        return DependencyModel(
            id,
            tableId,
            dependencyTargetId,
            dependencyTableId,
            table,
            dependencyTable,
            dependencyTarget,
            area,
            vertical,
            tableName,
            viewName,
            format,
            version
        )
    }

    override fun getThis() = this
}