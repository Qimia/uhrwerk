package io.qimia.uhrwerk.common.metastore.builders

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.*

class TargetModelBuilder : StateModelBuilder<TargetModelBuilder>() {
    var id: Long? = null
    var tableId: Long? = null
    var connectionId: Long? = null
    var connection: ConnectionModel? = null
    var table: TableModel? = null
    var format: String? = null
    var tableName: String? = null
    fun id(id: Long?): TargetModelBuilder {
        this.id = id
        return this
    }

    fun tableId(tableId: Long): TargetModelBuilder {
        this.tableId = tableId
        return this
    }

    fun connectionId(connectionId: Long): TargetModelBuilder {
        this.connectionId = connectionId
        return this
    }

    fun connection(connection: ConnectionModel?): TargetModelBuilder {
        this.connection = connection
        return this
    }

    fun table(table: TableModel?): TargetModelBuilder {
        this.table = table
        return this
    }

    fun format(format: String): TargetModelBuilder {
        this.format = format
        return this
    }

    fun tableName(tableName: String?): TargetModelBuilder {
        this.tableName = tableName
        return this
    }

    fun build(): TargetModel {
        return TargetModel(
            id,
            format,
            tableId,
            connectionId,
            tableName,
            connection,
            table
        )
    }

    override fun getThis() = this

}