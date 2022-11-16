package io.qimia.uhrwerk.common.metastore.builders

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import io.qimia.uhrwerk.common.metastore.model.SourceModel
import io.qimia.uhrwerk.common.metastore.model.TableModel

class SourceModelBuilder : StateModelBuilder<SourceModelBuilder>() {
    var id: Long? = null
    var tableId: Long? = null
    var connectionId: Long? = null
    var path: String? = null
    var format: String? = null
    var connection: ConnectionModel? = null
    var table: TableModel? = null
    var partitionUnit: PartitionUnit? = null
    var partitionSize = 0
    var parallelLoadQuery: String? = null
    var parallelLoadColumn: String? = null
    var parallelLoadNum = 0
    var selectQuery: String? = null
    var selectColumn: String? = null
    var isPartitioned = false
    var isAutoLoad = false
    fun id(id: Long?): SourceModelBuilder {
        this.id = id
        return this
    }

    fun tableId(tableId: Long): SourceModelBuilder {
        this.tableId = tableId
        return this
    }

    fun connectionId(connectionId: Long): SourceModelBuilder {
        this.connectionId = connectionId
        return this
    }

    fun path(path: String): SourceModelBuilder {
        this.path = path
        return this
    }

    fun format(format: String): SourceModelBuilder {
        this.format = format
        return this
    }

    fun connection(connection: ConnectionModel?): SourceModelBuilder {
        this.connection = connection
        return this
    }

    fun table(table: TableModel?): SourceModelBuilder {
        this.table = table
        return this
    }

    fun partitionUnit(partitionUnit: PartitionUnit?): SourceModelBuilder {
        this.partitionUnit = partitionUnit
        return this
    }

    fun partitionSize(partitionSize: Int): SourceModelBuilder {
        this.partitionSize = partitionSize
        return this
    }

    fun parallelLoadQuery(parallelLoadQuery: String?): SourceModelBuilder {
        this.parallelLoadQuery = parallelLoadQuery
        return this
    }

    fun parallelLoadColumn(parallelLoadColumn: String?): SourceModelBuilder {
        this.parallelLoadColumn = parallelLoadColumn
        return this
    }

    fun parallelLoadNum(parallelLoadNum: Int): SourceModelBuilder {
        this.parallelLoadNum = parallelLoadNum
        return this
    }

    fun selectQuery(selectQuery: String?): SourceModelBuilder {
        this.selectQuery = selectQuery
        return this
    }

    fun selectColumn(selectColumn: String?): SourceModelBuilder {
        this.selectColumn = selectColumn
        return this
    }

    fun partitioned(partitioned: Boolean): SourceModelBuilder {
        isPartitioned = partitioned
        return this
    }

    fun autoLoad(autoLoad: Boolean): SourceModelBuilder {
        isAutoLoad = autoLoad
        return this
    }

    fun build(): SourceModel {
        return SourceModel(
            id,
            tableId,
            connectionId,
            path,
            format,
            connection,
            table,
            partitionUnit,
            partitionSize,
            parallelLoadQuery,
            parallelLoadColumn,
            parallelLoadNum,
            selectQuery,
            selectColumn,
            isPartitioned,
            isAutoLoad
        )
    }

    override fun getThis() = this

}